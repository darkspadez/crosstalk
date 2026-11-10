//! Running ffprobe and ffmpeg, behind a trait.
//!
//! IPTV Checker called `subprocess.run` twice per stream (`check_stream` 4930, and
//! `_analyze_stream_content` 4846 for the one content pass). That is the ONLY thing a probe
//! needs from the outside world besides HTTP, so it is the one thing behind a seam: the action
//! asks a [`ToolRunner`] to run a [`ToolSpec`] and gets a [`ToolOutput`] back, which is exactly
//! the `subprocess.CompletedProcess` shape the plugin inspects (`returncode`, `stdout`,
//! `stderr`) plus the two outcomes Python expressed as exceptions (`TimeoutExpired`,
//! `FileNotFoundError`).
//!
//! Why a seam and not `tokio::process` at the call site: the plan's "probe without network"
//! requirement. The parity harness replays outputs recorded from the real tools, and its Rust
//! half has to reach the same parsing and classification code, so the runner has to be
//! swappable in a test. [`ProcessRunner`] is the real one; [`FakeToolRunner`] answers from a
//! table.
//!
//! Two behaviours the plugin did not have, both because a probe is the longest job crosstalk
//! runs:
//!
//!   * **the process is killed on timeout and on cancel.** `subprocess.run(timeout=…)` killed
//!     on its own timeout but ignored a cancelled run, so a cancelled check waited out the
//!     slowest stream. A cancelled probe kills the child within one poll interval;
//!   * **the executables are checked before the first probe** ([`ToolRunner::preflight`]).
//!     A missing ffprobe made IPTV Checker classify EVERY stream `Other`/Dead and clear its
//!     stored `stream_stats`, which is the worst possible failure mode of this action. The
//!     real runner refuses the run instead, naming the path.

use anyhow::{Context, Result};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often a running child re-reads the cancel flag.
///
/// The same interval the per-key limiter polls with (`crosstalk_core::limiter::CANCEL_POLL`),
/// so a cancelled run stops everywhere within one tick.
pub const CANCEL_POLL: Duration = Duration::from_millis(50);

/// How much of one pipe is kept: 8 MiB of ffprobe JSON is orders of magnitude past the
/// largest real `-show_packets` window (tens of kilobytes for 8 s), and a run holds one probe
/// per lane in memory at once, so an uncapped read would let one pathological stream set the
/// run's memory. Past the cap the reader keeps DRAINING (a full pipe would deadlock the child)
/// and discards, so the parse sees a truncated document and says `Other` rather than hanging
/// the run or the host.
pub const PIPE_CAP: usize = 8 * 1024 * 1024;

/// One tool invocation: what to run, with which arguments, and how long it may take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    /// The executable, from `streams.toml` (`ffprobe_path` / `ffmpeg_path`).
    pub program: String,
    /// Arguments, in order, exactly as the plugin built them.
    pub args: Vec<String>,
    /// Wall-clock cap. The plugin passed `subprocess.run(timeout=…)`.
    pub timeout: Duration,
    /// Which of `args` is the probed URL, fixed when the spec is built: the value of `-i`,
    /// or else the first argument carrying a scheme. Kept rather than re-guessed so the
    /// masked command line hides exactly the argument the tool reads as its input, with or
    /// without a scheme.
    url_index: Option<usize>,
}

impl ToolSpec {
    pub fn new(program: impl Into<String>, args: Vec<String>, timeout: Duration) -> Self {
        let url_index = args
            .iter()
            .position(|arg| arg == "-i")
            .map(|at| at + 1)
            .filter(|at| *at < args.len())
            .or_else(|| args.iter().position(|arg| arg.contains("://")));
        Self {
            program: program.into(),
            args,
            timeout,
            url_index,
        }
    }

    /// The command line, for the debug log and for the parity harness.
    ///
    /// Joined with spaces because that is how the plugin logged it (5057) and how an operator
    /// reproduces it by hand; arguments carrying spaces are printed as they are, like the
    /// plugin's `' '.join(cmd)`.
    pub fn command_line(&self) -> String {
        let mut line = self.program.clone();
        for arg in &self.args {
            line.push(' ');
            line.push_str(arg);
        }
        line
    }

    /// The command line with the probed URL redacted, for errors and logs.
    ///
    /// Provider URLs carry credentials, and an error is one paste into an issue away from
    /// public. Reproduction keeps the flags (which is what an operator re-runs by hand) and
    /// loses only the secret.
    pub fn masked_command_line(&self) -> String {
        let mut line = self.program.clone();
        for (index, arg) in self.args.iter().enumerate() {
            line.push(' ');
            if Some(index) == self.url_index {
                line.push_str("[stream-url]");
            } else {
                line.push_str(arg);
            }
        }
        line
    }
    /// The URL being probed: the value of `-i`, or else the one argument carrying a scheme.
    ///
    /// Not the last argument: both commands take the URL as `-i <url>` followed by more
    /// flags (`-t 6 … -f null -` for ffmpeg), so a fake keyed on "last" would key every ffmpeg
    /// call on `-`.
    pub fn url(&self) -> Option<&str> {
        self.url_index
            .and_then(|at| self.args.get(at))
            .map(String::as_str)
    }

    /// The executable's file name (`ffprobe` for `/usr/local/bin/ffprobe`), which is what the
    /// fake runner keys on so a test does not have to know the install path.
    pub fn program_name(&self) -> String {
        Path::new(&self.program)
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| self.program.clone())
    }
}

/// What one invocation produced.
///
/// `status` is `None` when the child never reported one (killed, or failed to start), which is
/// where Python's exceptions went: the plugin's classifier reads a *stderr* for those cases
/// anyway, and [`ToolOutput::success`] is false either way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolOutput {
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    /// `subprocess.TimeoutExpired`: the tool was killed at its cap.
    pub timed_out: bool,
    /// The run was cancelled while this invocation was in flight, so it was killed.
    pub cancelled: bool,
}

impl ToolOutput {
    /// A synthetic output for tests and fixtures.
    pub fn new(status: i32, stdout: impl Into<String>, stderr: impl Into<String>) -> Self {
        Self {
            status: Some(status),
            stdout: stdout.into(),
            stderr: stderr.into(),
            timed_out: false,
            cancelled: false,
        }
    }

    /// `returncode == 0`, which is the plugin's definition of a successful probe.
    pub fn success(&self) -> bool {
        self.status == Some(0) && !self.timed_out && !self.cancelled
    }
}

/// Anything that can run a tool.
///
/// The future is boxed by hand rather than through `async_trait`, matching [`crate::SubApp`]'s
/// own signature: one dependency less, and the object safety is obvious.
pub trait ToolRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        spec: &'a ToolSpec,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput>> + Send + 'a>>;

    /// Check the executables exist before the first probe.
    ///
    /// Default: nothing to check (a fake runner answers from a table). [`ProcessRunner`]
    /// refuses a missing binary, because IPTV Checker's failure mode — every stream Dead with
    /// `Other`, and every `stream_stats` cleared — is not one to reproduce.
    fn preflight(&self, _ffprobe: &str, _ffmpeg: Option<&str>) -> Result<()> {
        Ok(())
    }
}

/// Poll the cancel flag, resolving once it is set.
async fn until_cancelled(flag: &Arc<AtomicBool>) {
    loop {
        if flag.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(CANCEL_POLL).await;
    }
}

/// Read a child pipe to EOF, keeping the first `PIPE_CAP` bytes.
///
/// The cap is on MEMORY, not on the child: past it the reader keeps draining so a full pipe
/// never deadlocks the writer, and the excess is discarded. Callers see a truncated document,
/// which the ffprobe parse turns into `Other` rather than a hang or an allocation spike.
async fn read_capped<R>(pipe: &mut R) -> Vec<u8>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let room = PIPE_CAP.saturating_sub(buffer.len());
                buffer.extend_from_slice(&chunk[..read.min(room)]);
            }
        }
    }
    buffer
}

/// The real runner: `tokio::process`, killed at its timeout and when the run is cancelled.
#[derive(Debug, Clone)]
pub struct ProcessRunner {
    cancelled: Arc<AtomicBool>,
}

impl ProcessRunner {
    /// A runner tied to one run's cancel flag (the job's own —
    /// [`crosstalk_core::jobs::JobContext::cancel_flag`]).
    pub fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self { cancelled }
    }

    async fn exec(&self, spec: &ToolSpec) -> Result<ToolOutput> {
        use std::process::Stdio;

        let mut command = tokio::process::Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // The belt to the explicit kill's braces: a dropped child is killed, so even a
            // panic mid-probe cannot leave an ffprobe reading a provider stream.
            .kill_on_drop(true);
        // Its own process group, so a kill reaches every process it started: a tool run
        // through a shell (dash does not exec its last command) or one that forks would
        // otherwise leave a grandchild holding the output pipes open, and the reads below
        // would wait for it long after the timeout.
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("running {}", spec.masked_command_line()))?;
        // Taken now: once the child is reaped `id()` is gone, and the group may outlive it.
        let group = child.id();

        // Read both pipes while the child runs: ffprobe's stdout for 8 s of packets is far
        // larger than a pipe buffer, so reading after the wait would deadlock the child. Each
        // pipe is capped at `PIPE_CAP`: past it the reader drains and discards, so a
        // pathological stream cannot set the run's memory.
        let mut stdout_pipe = child.stdout.take().expect("piped stdout");
        let mut stderr_pipe = child.stderr.take().expect("piped stderr");
        let stdout_task = tokio::spawn(async move { read_capped(&mut stdout_pipe).await });
        let stderr_task = tokio::spawn(async move { read_capped(&mut stderr_pipe).await });

        let mut output = ToolOutput::default();
        let mut waited = Box::pin(child.wait());
        let deadline = tokio::time::sleep(spec.timeout);
        tokio::pin!(deadline);

        enum Stop {
            Exited(std::io::Result<std::process::ExitStatus>),
            TimedOut,
            Cancelled,
        }
        let stop = tokio::select! {
            status = &mut waited => Stop::Exited(status),
            _ = &mut deadline => Stop::TimedOut,
            _ = until_cancelled(&self.cancelled) => Stop::Cancelled,
        };
        // End the borrow of `child` that `waited` holds before killing it.
        drop(waited);

        match stop {
            Stop::Exited(status) => {
                output.status = status.context("waiting for the tool to finish")?.code();
                // A grandchild the tool left behind still holds the pipes open: end the group
                // so the reads below reach EOF instead of waiting on it.
                kill_group_id(group);
            }
            Stop::TimedOut => {
                output.timed_out = true;
                kill_group(&mut child).await;
            }
            Stop::Cancelled => {
                output.cancelled = true;
                kill_group(&mut child).await;
            }
        }

        // The pipes close once the group is gone; the bound is the backstop for a process
        // that escaped the group, so a probe never outlives its cap (or its exit) by more.
        let stdout = collect(stdout_task).await;
        let stderr = collect(stderr_task).await;
        output.stdout = String::from_utf8_lossy(&stdout).into_owned();
        output.stderr = String::from_utf8_lossy(&stderr).into_owned();
        Ok(output)
    }
}

/// How long the pipe readers may run on after the child stopped before what they hold is
/// given up.
const AFTER_KILL_READ: Duration = Duration::from_secs(2);

/// Signal every process in the group the child leads.
fn kill_group_id(group: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = group {
        // SAFETY: `killpg` only sends a signal; the group id is the child's pid because it
        // was spawned with `process_group(0)`, and a group with no members left fails
        // harmlessly with ESRCH (a pid is not reused while its group still has members).
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = group;
}

/// Kill the child and every process in its group, then reap it.
async fn kill_group(child: &mut tokio::process::Child) {
    kill_group_id(child.id());
    let _ = child.kill().await;
    let _ = child.wait().await;
}

/// A pipe reader's bytes, bounded by [`AFTER_KILL_READ`] once the child has stopped.
async fn collect(task: tokio::task::JoinHandle<Vec<u8>>) -> Vec<u8> {
    let abort = task.abort_handle();
    match tokio::time::timeout(AFTER_KILL_READ, task).await {
        Ok(bytes) => bytes.unwrap_or_default(),
        Err(_) => {
            abort.abort();
            Vec::new()
        }
    }
}

impl ToolRunner for ProcessRunner {
    fn run<'a>(
        &'a self,
        spec: &'a ToolSpec,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput>> + Send + 'a>> {
        Box::pin(self.exec(spec))
    }

    fn preflight(&self, ffprobe: &str, ffmpeg: Option<&str>) -> Result<()> {
        for (role, path) in [("ffprobe", Some(ffprobe)), ("ffmpeg", ffmpeg)] {
            let Some(path) = path else { continue };
            if !Path::new(path).is_file() {
                anyhow::bail!(
                    "{role} not found at {path}: a probe runs the real tools, so {role} must be \
                     installed where the daemon runs (the image carries it). Set {role}_path in \
                     streams.toml if it lives elsewhere."
                );
            }
            check_tool_file(role, path)?;
        }
        Ok(())
    }
}

/// The checks on a configured tool path that need no filesystem: absolute, and named for the
/// tool (`ffprobe`, `ffprobe.exe`). `settings::validate` runs these on every load and save.
///
/// The path is executed for every probe, so a path that names anything else (`/bin/sh`, a
/// script in a writable directory) is refused rather than run.
pub fn check_tool_path(role: &str, path: &str) -> Result<()> {
    let candidate = Path::new(path);
    if !candidate.is_absolute() {
        anyhow::bail!("{role}_path must be an absolute path, got {path:?}");
    }
    let name = candidate
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    if name != role && name != format!("{role}.exe") {
        anyhow::bail!(
            "{role}_path must name the {role} executable ({role} or {role}.exe), got {path:?}"
        );
    }
    Ok(())
}

/// [`check_tool_path`], plus what only the filesystem can answer: the file is executable and
/// not world-writable (anyone who can rewrite it runs code as the daemon).
pub fn check_tool_file(role: &str, path: &str) -> Result<()> {
    check_tool_path(role, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .with_context(|| format!("reading {role}_path {path}"))?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            anyhow::bail!("{role}_path {path} is not executable");
        }
        if mode & 0o002 != 0 {
            anyhow::bail!("{role}_path {path} is world-writable; refusing to run it");
        }
    }
    Ok(())
}

/// A runner that answers from a table, for tests and the parity harness.
///
/// Keyed by `(file name of the program, the URL)` — both are in the recorded corpus, and a key
/// that misses is a loud error rather than a silent "dead stream": a fixture the harness forgot
/// must not look like a provider fault.
#[derive(Debug, Default)]
pub struct FakeToolRunner {
    answers: Mutex<HashMap<(String, String), ToolOutput>>,
    /// Per-key sequences: each call consumes the next output, and the last one repeats.
    sequences: Mutex<HashMap<(String, String), VecDeque<ToolOutput>>>,
    calls: Mutex<Vec<ToolSpec>>,
    failure: Mutex<Option<String>>,
}

impl FakeToolRunner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Answer `program` (by file name) for `url` with this recorded output.
    pub fn answering(self, program: &str, url: &str, output: ToolOutput) -> Self {
        self.answers
            .lock()
            .expect("fake runner lock")
            .insert((program.to_string(), url.to_string()), output);
        self
    }

    /// Answer `program` for `url` with a SEQUENCE: one output per call, the last repeating.
    ///
    /// Retries are the reason this exists: the same stream must be able to fail and then
    /// succeed on the next attempt.
    pub fn answering_sequence(self, program: &str, url: &str, outputs: Vec<ToolOutput>) -> Self {
        self.sequences.lock().expect("fake runner lock").insert(
            (program.to_string(), url.to_string()),
            outputs.into_iter().collect(),
        );
        self
    }

    /// Make every invocation fail like a missing binary.
    pub fn failing(self, message: impl Into<String>) -> Self {
        *self.failure.lock().expect("fake runner lock") = Some(message.into());
        self
    }

    /// Every spec the run handed to the runner, in order.
    pub fn calls(&self) -> Vec<ToolSpec> {
        self.calls.lock().expect("fake runner lock").clone()
    }
}

impl ToolRunner for FakeToolRunner {
    fn run<'a>(
        &'a self,
        spec: &'a ToolSpec,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput>> + Send + 'a>> {
        let answer = {
            self.calls
                .lock()
                .expect("fake runner lock")
                .push(spec.clone());
            if let Some(message) = self.failure.lock().expect("fake runner lock").clone() {
                return Box::pin(async move { Err(anyhow::anyhow!("{message}")) });
            }
            let key = (
                spec.program_name(),
                spec.url().unwrap_or_default().to_string(),
            );
            if let Some(sequence) = self
                .sequences
                .lock()
                .expect("fake runner lock")
                .get_mut(&key)
            {
                // The last output repeats, so a retry loop with more attempts than scripted
                // steps still terminates with a defined answer.
                let output = if sequence.len() > 1 {
                    sequence.pop_front()
                } else {
                    sequence.front().cloned()
                };
                output
            } else {
                self.answers
                    .lock()
                    .expect("fake runner lock")
                    .get(&key)
                    .cloned()
            }
        };
        Box::pin(async move {
            match answer {
                Some(output) => Ok(output),
                // Masked: a fixture table keyed by URL would otherwise print a provider
                // credential into a test failure.
                None => anyhow::bail!(
                    "the fake runner has no recorded output for {}",
                    spec.masked_command_line()
                ),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spec_names_its_program_target_and_command_line() {
        let spec = ToolSpec::new(
            "/usr/local/bin/ffprobe",
            vec!["-print_format".to_string(), "http://host/a.ts".to_string()],
            Duration::from_secs(5),
        );
        assert_eq!(spec.program_name(), "ffprobe");
        assert_eq!(spec.url(), Some("http://host/a.ts"));
        assert_eq!(
            spec.command_line(),
            "/usr/local/bin/ffprobe -print_format http://host/a.ts"
        );
    }

    #[test]
    fn the_masked_command_line_redacts_only_the_url() {
        let spec = ToolSpec::new(
            "/usr/bin/ffprobe",
            vec![
                "-timeout".to_string(),
                "10000000".to_string(),
                "http://user:pass@host/live/1.ts?token=secret".to_string(),
            ],
            Duration::from_secs(5),
        );
        let masked = spec.masked_command_line();
        assert!(masked.contains("-timeout 10000000"), "{masked}");
        assert!(!masked.contains("secret"), "{masked}");
        assert!(!masked.contains("user:pass"), "{masked}");
        assert!(masked.contains("[stream-url]"), "{masked}");
        // No scheme anywhere, nothing to redact: unchanged.
        let plain = ToolSpec::new(
            "/bin/sh",
            vec!["-c".to_string(), "sleep 1".to_string()],
            Duration::from_secs(5),
        );
        assert_eq!(plain.masked_command_line(), plain.command_line());

        // The input is masked by position, so a scheme-less input is hidden too and an
        // unrelated argument with `://` is not mistaken for it.
        let spec = ToolSpec::new(
            "/usr/bin/ffmpeg",
            vec![
                "-headers".to_string(),
                "Referer: http://site.test/".to_string(),
                "-i".to_string(),
                "provider.test/live/u/p/1.ts".to_string(),
            ],
            Duration::from_secs(5),
        );
        assert_eq!(spec.url(), Some("provider.test/live/u/p/1.ts"));
        assert_eq!(
            spec.masked_command_line(),
            "/usr/bin/ffmpeg -headers Referer: http://site.test/ -i [stream-url]"
        );
    }

    #[tokio::test]
    async fn a_capped_read_keeps_the_head_and_drains_the_rest() {
        // More than the cap, from a pipe: the reader keeps 8 MiB and still reaches EOF, so a
        // child writing past it is never deadlocked — and the run never holds the whole thing.
        let (mut writer, mut reader) = tokio::io::duplex(65536);
        let body = vec![b'x'; PIPE_CAP + 1_000_000];
        let writing = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut written = 0;
            while written < body.len() {
                let end = (written + 8192).min(body.len());
                writer.write_all(&body[written..end]).await.expect("write");
                written = end;
            }
            drop(writer);
        });
        let kept = read_capped(&mut reader).await;
        writing
            .await
            .expect("the writer finishes: it was never blocked");
        assert_eq!(kept.len(), PIPE_CAP);
    }

    #[test]
    fn success_is_a_zero_exit_that_neither_timed_out_nor_was_cancelled() {
        assert!(ToolOutput::new(0, "{}", "").success());
        assert!(!ToolOutput::new(1, "", "boom").success());
        // Python's TimeoutExpired never had a returncode; a killed child may report one, and
        // neither is a success.
        let killed = ToolOutput {
            status: Some(0),
            timed_out: true,
            ..ToolOutput::default()
        };
        assert!(!killed.success());
        let cancelled = ToolOutput {
            status: Some(0),
            cancelled: true,
            ..ToolOutput::default()
        };
        assert!(!cancelled.success());
        assert!(!ToolOutput::default().success(), "no status is not success");
    }

    #[tokio::test]
    async fn the_fake_runner_replays_by_program_and_url_and_records_the_calls() {
        let runner = FakeToolRunner::new().answering(
            "ffprobe",
            "http://host/a.ts",
            ToolOutput::new(0, "{\"streams\":[]}", ""),
        );
        let spec = ToolSpec::new(
            "/opt/bin/ffprobe",
            vec!["-v".to_string(), "http://host/a.ts".to_string()],
            Duration::from_secs(5),
        );
        let output = runner.run(&spec).await.expect("recorded");
        assert_eq!(output.stdout, "{\"streams\":[]}");
        assert_eq!(runner.calls().len(), 1);
        assert_eq!(runner.calls()[0].program_name(), "ffprobe");
        // A fake has nothing to preflight: a test must not need a real install.
        runner
            .preflight("/no/such/ffprobe", Some("/no/such/ffmpeg"))
            .expect("no-op");
    }

    #[tokio::test]
    async fn a_missing_fixture_is_an_error_not_a_dead_stream() {
        let runner = FakeToolRunner::new();
        let spec = ToolSpec::new(
            "/opt/bin/ffprobe",
            vec!["-v".into()],
            Duration::from_millis(10),
        );
        let error = runner.run(&spec).await.expect_err("no recorded output");
        assert!(format!("{error}").contains("no recorded output"), "{error}");
    }

    #[tokio::test]
    async fn the_real_runner_refuses_a_missing_binary_before_probing_anything() {
        let runner = ProcessRunner::new(Arc::new(AtomicBool::new(false)));
        let error = runner
            .preflight("/nonexistent/ffprobe", Some("/nonexistent/ffmpeg"))
            .expect_err("refuses");
        assert!(format!("{error}").contains("ffprobe not found"), "{error}");
        // ffmpeg is only required when a content detector is on, which the caller expresses by
        // passing `None`.
        let directory = tempfile::tempdir().expect("tempdir");
        let ffprobe = fake_tool(directory.path(), "ffprobe", 0o755);
        assert!(ProcessRunner::new(Arc::new(AtomicBool::new(false)))
            .preflight(&ffprobe, None)
            .is_ok());
    }

    /// An executable file named `name` with `mode`, for the path checks.
    fn fake_tool(directory: &Path, name: &str, mode: u32) -> String {
        let path = directory.join(name);
        std::fs::write(&path, "#!/bin/sh\n").expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        }
        #[cfg(not(unix))]
        let _ = mode;
        path.display().to_string()
    }

    #[test]
    fn a_tool_path_must_be_absolute_and_named_for_the_tool() {
        for path in [
            "/usr/bin/ffprobe",
            "/opt/homebrew/bin/ffprobe",
            "/x/FFPROBE.exe",
        ] {
            check_tool_path("ffprobe", path).unwrap_or_else(|e| panic!("{path}: {e}"));
        }
        check_tool_path("ffmpeg", "/usr/bin/ffmpeg").expect("ffmpeg");
        for path in [
            "/bin/sh",
            "ffprobe",
            "bin/ffprobe",
            "/usr/bin/ffmpeg",
            "/usr/bin/ffprobe.sh",
            "/usr/bin/",
            "",
        ] {
            assert!(check_tool_path("ffprobe", path).is_err(), "{path:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_tool_file_must_be_executable_and_not_world_writable() {
        let directory = tempfile::tempdir().expect("tempdir");
        let runner = ProcessRunner::new(Arc::new(AtomicBool::new(false)));
        let good = fake_tool(directory.path(), "ffprobe", 0o755);
        runner
            .preflight(&good, None)
            .expect("a normal install passes");

        let plain = tempfile::tempdir().expect("tempdir");
        let not_executable = fake_tool(plain.path(), "ffprobe", 0o644);
        let error = runner
            .preflight(&not_executable, None)
            .expect_err("refused");
        assert!(format!("{error}").contains("not executable"), "{error}");

        let open = tempfile::tempdir().expect("tempdir");
        let writable = fake_tool(open.path(), "ffprobe", 0o777);
        let error = runner.preflight(&writable, None).expect_err("refused");
        assert!(format!("{error}").contains("world-writable"), "{error}");

        // A real binary under another name is refused before anything runs it.
        let error = runner.preflight("/bin/sh", None).expect_err("refused");
        assert!(format!("{error}").contains("ffprobe_path"), "{error}");
    }

    /// An opt-in check that the real runner actually runs a real process, times it out and
    /// kills it. Uses `/bin/sh` so it needs no ffmpeg; runs everywhere, unlike the ffprobe test.
    #[tokio::test]
    async fn the_real_runner_captures_output_and_kills_at_its_timeout() {
        let runner = ProcessRunner::new(Arc::new(AtomicBool::new(false)));
        let spec = ToolSpec::new(
            "/bin/sh",
            vec!["-c".to_string(), "printf out; printf err >&2".to_string()],
            Duration::from_secs(5),
        );
        let output = runner.run(&spec).await.expect("ran");
        assert_eq!(output.stdout, "out");
        assert_eq!(output.stderr, "err");
        assert_eq!(output.status, Some(0));
        assert!(output.success());

        // A child that outlives its cap is killed, and says so.
        let slow = ToolSpec::new(
            "/bin/sh",
            // `; true` keeps the shell from exec-ing `sleep`, so the group kill is exercised
            // (dash never execs; bash does for a lone last command).
            vec!["-c".to_string(), "sleep 30; true".to_string()],
            Duration::from_millis(50),
        );
        let started = std::time::Instant::now();
        let output = runner.run(&slow).await.expect("ran");
        assert!(output.timed_out);
        assert!(!output.success());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the cap is enforced, not waited out"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_grandchild_left_holding_the_pipes_does_not_outlive_a_normal_exit() {
        let runner = ProcessRunner::new(Arc::new(AtomicBool::new(false)));
        // The shell exits at once; the backgrounded sleep inherits stdout and would keep the
        // read open for 30 s if the group were not ended after the exit.
        let spec = ToolSpec::new(
            "/bin/sh",
            vec!["-c".to_string(), "sleep 30 & printf done".to_string()],
            Duration::from_secs(60),
        );
        let started = std::time::Instant::now();
        let output = runner.run(&spec).await.expect("ran");
        assert_eq!(output.status, Some(0));
        assert_eq!(output.stdout, "done");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the reads ended with the group, not with the sleep"
        );
    }

    #[tokio::test]
    async fn the_real_runner_kills_a_child_when_the_run_is_cancelled() {
        let flag = Arc::new(AtomicBool::new(false));
        let runner = ProcessRunner::new(Arc::clone(&flag));
        let spec = ToolSpec::new(
            "/bin/sh",
            // `; true` keeps the shell from exec-ing `sleep`, so the group kill is exercised
            // (dash never execs; bash does for a lone last command).
            vec!["-c".to_string(), "sleep 30; true".to_string()],
            Duration::from_secs(60),
        );
        let waiting = tokio::spawn(async move { runner.run(&spec).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        flag.store(true, Ordering::SeqCst);
        let output = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the cancel reaches the child")
            .expect("no panic")
            .expect("ran");
        assert!(output.cancelled);
        assert!(!output.success());
    }
}
