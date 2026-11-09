//! `bench`: the timing tool the plan promised, and the evidence behind the README's numbers.
//!
//! The plan makes scale a first-class requirement (20k channels, 50k streams, ~100 accounts)
//! and leaves one architectural question to the operator: whether the REST stream-list writer
//! is fast enough, or whether a Dispatcharr plugin (the bridge) has to write `ChannelStream`
//! rows in one transaction after all. Both answers are measurements, so both are taken here.
//!
//! Two modes:
//!
//!   * **synthetic** ([`synthetic`]) builds a world in memory — 20,000 channels, 50,000
//!     streams, 100 accounts unless told otherwise — and times the four plans this app has
//!     (match, sort, mirror, visibility), the shared write plan behind each (`channel_ops::
//!     plan`) and the number of write requests an apply would issue. No network, no
//!     Dispatcharr, no store: the numbers say what this machine does with this much data.
//!   * **live** ([`live`]) runs the same four plans against the Dispatcharr the configuration
//!     names, and times [`DispatcharrClient::load_world_with`] as well — the half of a run a
//!     synthetic world cannot price. It is READ-ONLY unless `--write-timing` asks for more.
//!
//! `--write-timing` is the one thing here that writes, and it writes nothing that changes:
//! each sampled channel's stream list is read back just before the write and rewritten to
//! that value (the same idempotent no-op the writer's retry relies on), once per channel
//! and once as one `edit/bulk` chunk, so the two request patterns can be compared. It needs
//! `--yes` on top of the flag, because a bench that writes without being asked twice is a
//! trap.
//!
//! # What "write requests" means
//!
//! The REST writer sends stream lists as `PATCH /channels/channels/edit/bulk/` rows in chunks
//! of [`WRITE_CHUNK`] (500). So an apply of `n` changed channels costs `ceil(n / 500)`
//! requests, not `n` — that is the whole argument for the bulk path — and a rejected chunk
//! costs one PATCH per channel in it, which no bench can predict. Every `requests` number
//! below therefore counts the happy path and prints the `writes` it came from, so a reader can
//! see both.
//!
//! # Debug builds
//!
//! A debug build is 10-50× slower than a release build on exactly the code these plans run,
//! so a number taken from one is worse than no number: it would be quoted as if it were the
//! release figure. The report says so in a note when `cfg!(debug_assertions)` is set.

use crate::matching::{self, ChannelDatabase, MatchOptions};
use crate::mirror;
use crate::quality::{MemoryThroughputSource, ThroughputSource};
use crate::settings::StreamsSettings;
use crate::sort::{self, SortOptions};
use crate::visibility;
use anyhow::{bail, Context, Result};
use crosstalk_core::channel_ops::{self, Field, Op, Selection, SetValue};
use crosstalk_core::config::{CrosstalkConfig, MatchingConfig, ScopeConfig};
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, ChannelProfile, DispatcharrClient, M3uAccount, Stream, World, WorldKind,
    WorldKinds, CHANNEL_EDIT_CHUNK_SIZE,
};
use crosstalk_core::store::{RunStatus, Store};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Channels per write request: the writer's own chunk size, imported rather than restated so
/// the bench cannot drift from what [`crosstalk_core::channel_ops::RestStreamListWriter`]
/// actually sends.
pub const WRITE_CHUNK: usize = CHANNEL_EDIT_CHUNK_SIZE;

/// The budget the bridge decision is measured against: a 20,000-channel apply that fits in
/// this is "REST is enough", and one that does not is what a bridge would buy.
pub const APPLY_BUDGET: Duration = Duration::from_secs(120);

/// The world size a synthetic run builds by default: the plan's own numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticOptions {
    pub channels: usize,
    pub streams: usize,
    pub accounts: usize,
}

impl Default for SyntheticOptions {
    fn default() -> Self {
        Self {
            channels: 20_000,
            streams: 50_000,
            accounts: 100,
        }
    }
}

impl SyntheticOptions {
    /// The same size, refused when a number is nonsense (a zero-channel world measures
    /// nothing, and `--channels 0` is a typo, not a request).
    pub fn checked(self) -> Result<Self> {
        if self.channels == 0 || self.streams == 0 || self.accounts == 0 {
            bail!(
                "a synthetic world needs at least one channel, stream and account (got {} \
                 channel(s), {} stream(s), {} account(s))",
                self.channels,
                self.streams,
                self.accounts
            );
        }
        Ok(self)
    }

    pub fn describe(&self) -> String {
        format!(
            "synthetic world: {} channel(s), {} stream(s), {} account(s)",
            thousands(self.channels),
            thousands(self.streams),
            thousands(self.accounts)
        )
    }
}

// =======================================================================================
// The synthetic world
// =======================================================================================

/// Network tokens the synthetic channel names are built from: enough families that the name
/// index has real work to do, and names that look like a lineup rather than `channel-1`.
const NETWORKS: [&str; 24] = [
    "ESPN",
    "CNN",
    "HBO",
    "Discovery",
    "Bravo",
    "TNT",
    "AMC",
    "Syfy",
    "Nickelodeon",
    "Cartoon Network",
    "History",
    "Lifetime",
    "A&E",
    "USA",
    "TBS",
    "FX",
    "Hallmark",
    "Nat Geo",
    "Food Network",
    "HGTV",
    "Travel",
    "Animal Planet",
    "Paramount",
    "Showtime",
];

/// Channel groups the synthetic world carries.
const CHANNEL_GROUPS: usize = 20;
/// Provider feeds per name family, each duplicated on a second host of the same server group.
const FEEDS_PER_FAMILY: usize = 5;
/// Streams per name family: [`FEEDS_PER_FAMILY`] on each of two hosts.
const STREAMS_PER_FAMILY: usize = FEEDS_PER_FAMILY * 2;
/// The name family a synthetic channel's own list is drawn from, plus the next family: ten
/// ids per channel, which is the "20k channels × 10 streams" the plan's sort estimate uses.
const STREAMS_PER_CHANNEL: usize = 10;

/// The synthetic name family `index`, in a lineup's shape: `ESPN 3`, `HBO 12 West`.
///
/// A zone suffix on some families is deliberate: zone routing is a real part of a sort
/// (`_zone_routed_map`), and a bench that never exercises it would miss the pass.
fn family_name(index: usize) -> String {
    let network = NETWORKS[index % NETWORKS.len()];
    let slot = index / NETWORKS.len();
    let zone = if index > 0 && index % 13 == 0 {
        " West"
    } else if index > 0 && index % 17 == 0 {
        " East"
    } else {
        ""
    };
    format!("{network} {slot}{zone}")
}

/// The `stream_stats` of feed `feed`: a 4K, an HD, a 720p, an SD and one with no stats at all.
///
/// The spread is what makes a sort non-trivial (five distinct keys per family), and the empty
/// one is the plugin's worst case: a stream that states no resolution at all.
fn feed_stats(feed: usize) -> serde_json::Value {
    let (width, height, fps, bitrate) = match feed % FEEDS_PER_FAMILY {
        0 => (3840, 2160, 50.0, Some(9000)),
        1 => (1920, 1080, 60.0, Some(2656)),
        2 => (1280, 720, 60.0, Some(4000)),
        3 => (720, 576, 25.0, Some(1200)),
        _ => return serde_json::json!({}),
    };
    let mut payload = serde_json::json!({
        "width": width,
        "height": height,
        "source_fps": fps,
        "video_codec": "h264",
    });
    if let Some(bitrate) = bitrate {
        payload["video_bitrate"] = serde_json::json!(bitrate);
    }
    payload
}

/// The accounts of one provider's server group, as indices into the account list.
fn group_members(index: usize, accounts: usize, groups: usize) -> Vec<usize> {
    (0..accounts)
        .filter(|account| account % groups == index % groups)
        .collect()
}

/// A world of the requested size, built to make every plan do real work.
///
/// The shape, and why each part is there:
///
///   * **accounts** are round-robin into server groups (`account % groups`), each with its own
///     `server_url`, so accounts in a group are mirrors of one another (mirror fill) and the
///     account priority order is real (sort);
///   * **name families** are `streams / 10`: five provider feeds, each carried by TWO accounts
///     of the same server group under the same provider id — which is exactly what a mirror
///     fill exists to copy;
///   * **channels** are distributed over the families, several per name (`visible_channel_limit`
///     decides how many of them a match writes), each holding ten streams: its own family's
///     five feeds plus the next family's five, in an order the quality model would not choose.
///     Without a second family a channel's current order could already be the answer, and the
///     sort would time a no-op.
///
/// Nothing is random: two runs over the same options plan the same world, so two numbers can
/// be compared.
pub fn synthetic_world(options: &SyntheticOptions) -> World {
    let accounts = options.accounts;
    let groups = accounts.div_ceil(10).max(1);
    let families = (options.streams / STREAMS_PER_FAMILY).max(1);

    let mut world = World::default();
    for index in 0..accounts {
        world.m3u_accounts.push(M3uAccount {
            id: index as i64 + 1,
            name: format!("Provider {}", index + 1),
            is_active: true,
            account_type: if index % 2 == 0 { "XC" } else { "STD" }.to_string(),
            max_streams: Some(FEEDS_PER_FAMILY as i64),
            priority: index as i64,
            server_url: Some(format!("http://provider-{}.test", index + 1)),
            server_group: Some((index % groups) as i64 + 1),
            ..M3uAccount::default()
        });
    }
    for group in 0..CHANNEL_GROUPS {
        world.groups.push(ChannelGroup {
            id: 100 + group as i64,
            name: format!("Group {}", group + 1),
        });
    }

    // The streams: every feed of every family, on two accounts of the family's server group,
    // under the same provider stream id.
    let mut next_stream_id = 1i64;
    let mut family_primaries: Vec<Vec<i64>> = Vec::with_capacity(families);
    for family in 0..families {
        let name = family_name(family);
        let members = group_members(family, accounts, groups);
        let mut primaries: Vec<i64> = Vec::with_capacity(FEEDS_PER_FAMILY);
        for feed in 0..FEEDS_PER_FAMILY {
            let provider_id = (family * FEEDS_PER_FAMILY + feed) as i64;
            for host in 0..2 {
                // The second host is half the group away, so it is a different account of the
                // same server group whenever the group has more than one member.
                let member = if members.len() > 1 {
                    members[(feed + host * (members.len() / 2)) % members.len()]
                } else {
                    members[0]
                };
                let id = next_stream_id;
                next_stream_id += 1;
                if host == 0 {
                    primaries.push(id);
                }
                world.streams.push(Stream {
                    id,
                    name: name.clone(),
                    stream_id: Some(provider_id),
                    m3u_account: Some(member as i64 + 1),
                    channel_group: Some(100 + (family % CHANNEL_GROUPS) as i64),
                    stream_stats: feed_stats(feed),
                    ..Stream::default()
                });
            }
        }
        family_primaries.push(primaries);
    }

    // The channels: `families` names, a few channels each, ten streams apiece in an order the
    // sort has to change (the next family's feeds come first, then this family's, reversed).
    let channels_per_family = options.channels.div_ceil(families).max(1);
    let mut channel_id = 1i64;
    for (family, primaries) in family_primaries.iter().enumerate() {
        let next = &family_primaries[(family + 1) % families];
        let mut streams: Vec<i64> = next.iter().rev().copied().collect();
        streams.extend(primaries.iter().rev().copied());
        streams.truncate(STREAMS_PER_CHANNEL);
        for _ in 0..channels_per_family {
            if world.channels.len() >= options.channels {
                break;
            }
            world.channels.push(Channel {
                id: channel_id,
                name: family_name(family),
                channel_number: Some(serde_json::json!(channel_id)),
                channel_group_id: Some(100 + (family % CHANNEL_GROUPS) as i64),
                streams: streams.clone(),
                ..Channel::default()
            });
            channel_id += 1;
        }
        if world.channels.len() >= options.channels {
            break;
        }
    }

    // One profile holding every channel: visibility writes a profile's membership, and a
    // world without one cannot be measured at all.
    world.profiles.push(ChannelProfile {
        id: 1,
        name: "All".to_string(),
        channels: world.channels.iter().map(|channel| channel.id).collect(),
    });
    world
}

// =======================================================================================
// The report
// =======================================================================================

/// One plan, timed.
#[derive(Debug, Clone)]
pub struct BenchPhase {
    /// The action the phase belongs to (`match`, `sort`, `mirror`, `visibility`).
    pub action: String,
    /// What a run does before any channel is looked at: the matcher and the channel databases
    /// (~10k entries and the FCC callsign file), which is start-up, not planning.
    pub setup_time: Option<Duration>,
    /// The verb's own plan: the part a run spends on rayon, with no I/O.
    pub plan_time: Option<Duration>,
    /// The shared write plan behind it (`channel_ops::plan`), or `None` when the verb's plan
    /// failed before there was anything to plan a write from.
    pub write_plan_time: Option<Duration>,
    /// Why the phase could not be measured. Not an error of the bench: a live install
    /// without a `[scope] profiles` entry simply cannot time a visibility plan.
    pub error: Option<String>,
    /// What the plan decided, in words.
    pub detail: String,
    /// Channels whose value would change: what an apply would write.
    pub writes: usize,
    /// The write requests those writes cost, at [`WRITE_CHUNK`] channels per request.
    pub requests: usize,
}

impl BenchPhase {
    fn failed(action: &str, error: impl std::fmt::Display) -> Self {
        Self {
            action: action.to_string(),
            setup_time: None,
            plan_time: None,
            write_plan_time: None,
            error: Some(format!("{error:#}")),
            detail: String::new(),
            writes: 0,
            requests: 0,
        }
    }

    /// One report line.
    pub fn line(&self) -> String {
        if let Some(error) = &self.error {
            return format!("{:<11} not measurable: {error}", self.action);
        }
        format!(
            "{:<11} setup {:>9}  plan {:>9}  write plan {:>9}  {:>7} write(s) {:>5} request(s)  \
             {}",
            self.action,
            seconds(self.setup_time),
            seconds(self.plan_time),
            seconds(self.write_plan_time),
            thousands(self.writes),
            thousands(self.requests),
            self.detail
        )
    }
}

/// The write patterns `--write-timing` measured, and what they say about 20,000 channels.
#[derive(Debug, Clone)]
pub struct WriteTiming {
    /// Channels sampled (one request each on the per-channel path).
    pub sample: usize,
    /// One PATCH per channel, all of them.
    pub per_channel: Duration,
    /// One `edit/bulk` request carrying the same lists.
    pub bulk: Duration,
    /// Writes the server refused (a refused write would make the timing a lie about speed).
    pub failed: usize,
    /// The install size the two are extrapolated to.
    pub extrapolated: usize,
}

impl WriteTiming {
    /// The sample scaled linearly to `extrapolated` channels.
    fn scale(&self, measured: Duration) -> Duration {
        if self.sample == 0 {
            return Duration::ZERO;
        }
        measured.mul_f64(self.extrapolated as f64 / self.sample as f64)
    }

    pub fn per_channel_estimate(&self) -> Duration {
        self.scale(self.per_channel)
    }

    pub fn bulk_estimate(&self) -> Duration {
        self.scale(self.bulk)
    }

    /// The rule the plan states, applied: REST is enough when an apply of
    /// [`WriteTiming::extrapolated`] channels fits in [`APPLY_BUDGET`]; otherwise the bridge
    /// has to be built.
    pub fn recommendation(&self) -> String {
        if self.failed > 0 {
            return format!(
                "{} write(s) were refused, so these timings are not the whole story: fix the \
                 sample (ids that exist, an administrator's key) and run it again",
                self.failed
            );
        }
        let bulk = self.bulk_estimate();
        let per_channel = self.per_channel_estimate();
        if bulk <= APPLY_BUDGET {
            format!(
                "REST is enough: one bulk apply of {} channels extrapolates to {} (budget {}), \
                 {}× less than one PATCH per channel and inside the budget. The bridge is not \
                 needed for stream-list writes.",
                thousands(self.extrapolated),
                seconds(Some(bulk)),
                seconds(Some(APPLY_BUDGET)),
                if per_channel.as_secs_f64() > 0.0 && bulk.as_secs_f64() > 0.0 {
                    format!("{:.0}", per_channel.as_secs_f64() / bulk.as_secs_f64())
                } else {
                    "?".to_string()
                }
            )
        } else {
            format!(
                "over the budget: one bulk apply of {} channels extrapolates to {} (budget {}). \
                 The bridge is worth building for stream-list writes.",
                thousands(self.extrapolated),
                seconds(Some(bulk)),
                seconds(Some(APPLY_BUDGET))
            )
        }
    }

    pub fn lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!(
                "write timing, {} channel(s) sampled, each list read back just before the \
                 write and rewritten to that value (change: none)",
                thousands(self.sample)
            ),
            format!(
                "  one PATCH per channel       {:>9}  → {} channels {:>9}",
                seconds(Some(self.per_channel)),
                thousands(self.extrapolated),
                seconds(Some(self.per_channel_estimate()))
            ),
            format!(
                "  one edit/bulk chunk         {:>9}  → {} channels {:>9}  ({} request(s) at \
                 {} rows each)",
                seconds(Some(self.bulk)),
                thousands(self.extrapolated),
                seconds(Some(self.bulk_estimate())),
                thousands(requests_for(self.extrapolated)),
                thousands(WRITE_CHUNK)
            ),
        ];
        lines.push(
            "  both figures are the sample scaled linearly by channel count; the bulk one is an \
             upper bound, because a request also pays a fixed cost the sample amortises"
                .to_string(),
        );
        lines.push(format!("  {}", self.recommendation()));
        lines
    }
}

/// What a bench run measured.
#[derive(Debug, Clone)]
pub struct BenchReport {
    pub title: String,
    /// Work that happens before a plan can start, timed separately because it is not the
    /// plan's cost (the synthetic build, or the live world load).
    pub setup: Vec<(String, Duration)>,
    pub phases: Vec<BenchPhase>,
    pub write_timing: Option<WriteTiming>,
    pub notes: Vec<String>,
}

impl BenchReport {
    /// The whole report as lines, which is what `crosstalk bench streams` prints.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = vec![self.title.clone()];
        for (label, elapsed) in &self.setup {
            lines.push(format!("  {label:<27} {:>9}", seconds(Some(*elapsed))));
        }
        for phase in &self.phases {
            lines.push(format!("  {}", phase.line()));
        }
        if let Some(timing) = &self.write_timing {
            lines.extend(timing.lines());
        }
        for note in &self.notes {
            lines.push(format!("  note: {note}"));
        }
        lines
    }

    pub fn print(&self) {
        for line in self.lines() {
            println!("{line}");
        }
    }
}

/// The write requests an apply of `writes` changed channels issues: the writer's own chunking.
pub fn requests_for(writes: usize) -> usize {
    writes.div_ceil(WRITE_CHUNK)
}

// =======================================================================================
// The phases
// =======================================================================================

/// The database codes a run resolves from `[matching]`.
fn database_codes(matching: &MatchingConfig) -> ChannelDatabase {
    let codes: Vec<String> = matching.databases();
    let codes: Vec<&str> = codes.iter().map(String::as_str).collect();
    ChannelDatabase::shipped(&codes)
}

/// The shared write plan for one verb's per-channel values: what an apply would write, and
/// what planning those writes costs (the `channel_ops` stage every channel write shares).
fn plan_write(
    world: &World,
    scope: &ScopeConfig,
    field: Field,
    values: HashMap<i64, SetValue>,
) -> (usize, Option<Duration>, Option<String>) {
    let started = Instant::now();
    let planned = channel_ops::plan(
        world,
        &Selection::in_scope(scope),
        &[Op::Set { field, values }],
    );
    let elapsed = started.elapsed();
    match planned {
        Ok(plan) => (plan.changes.len(), Some(elapsed), None),
        Err(error) => (0, Some(elapsed), Some(format!("{error:#}"))),
    }
}

fn match_phase(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    matching_config: &MatchingConfig,
    database: &ChannelDatabase,
    throughput: &dyn ThroughputSource,
) -> BenchPhase {
    let started = Instant::now();
    let options = MatchOptions::with_database(settings, matching_config, Some(database.clone()));
    let setup_time = started.elapsed();
    let started = Instant::now();
    let planned = matching::plan(world, scope, settings, &options, throughput);
    let plan_time = started.elapsed();
    let outcome = match planned {
        Ok(outcome) => outcome,
        Err(error) => return BenchPhase::failed("match", error),
    };

    // The same values `matching::run` builds: one ordered list per channel that changed.
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for assignment in outcome.writes() {
        values.insert(
            assignment.channel_id,
            SetValue::Streams(assignment.after.clone()),
        );
    }
    let (writes, write_plan_time, error) = plan_write(world, scope, Field::Streams, values);
    BenchPhase {
        action: "match".to_string(),
        setup_time: Some(setup_time),
        plan_time: Some(plan_time),
        write_plan_time,
        error,
        detail: format!(
            "{} channel(s) in {} name group(s), {} matched at least one stream, {} stream(s) \
             in the pool",
            thousands(outcome.channels),
            thousands(outcome.groups),
            thousands(outcome.updated_count()),
            thousands(outcome.pool.eligible)
        ),
        writes,
        requests: requests_for(writes),
    }
}

fn sort_phase(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    matching_config: &MatchingConfig,
    throughput: &dyn ThroughputSource,
) -> BenchPhase {
    let options = SortOptions::new(settings, world, matching_config);
    let started = Instant::now();
    let planned = sort::orders(world, scope, &options, throughput);
    let plan_time = started.elapsed();
    let result = match planned {
        Ok(result) => result,
        Err(error) => return BenchPhase::failed("sort", error),
    };

    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for order in result.changed() {
        values.insert(order.channel_id, SetValue::Streams(order.after.clone()));
    }
    let (writes, write_plan_time, error) = plan_write(world, scope, Field::Streams, values);
    BenchPhase {
        action: "sort".to_string(),
        setup_time: None,
        plan_time: Some(plan_time),
        write_plan_time,
        error,
        detail: format!(
            "{} channel(s), {} with more than one stream, {} already in order, {} stream(s) \
             dropped as the other zone",
            thousands(result.channels),
            thousands(result.candidates),
            thousands(result.unchanged_count()),
            thousands(result.dropped_by_zone())
        ),
        writes,
        requests: requests_for(writes),
    }
}

fn mirror_phase(world: &World, scope: &ScopeConfig) -> BenchPhase {
    let started = Instant::now();
    let planned = mirror::plan(world, scope);
    let plan_time = started.elapsed();
    let outcome = match planned {
        Ok(outcome) => outcome,
        Err(error) => return BenchPhase::failed("mirror", error),
    };

    // Channels a dangling stream id would leave alone are planned but never written
    // (`outcome.skipped`), exactly as `mirror::run` leaves them.
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for entry in &outcome.entries {
        values.insert(entry.channel_id, SetValue::Streams(entry.after.clone()));
    }
    let (writes, write_plan_time, error) = plan_write(world, scope, Field::Streams, values);
    BenchPhase {
        action: "mirror".to_string(),
        setup_time: None,
        plan_time: Some(plan_time),
        write_plan_time,
        error,
        detail: format!(
            "{} channel(s), {} would gain a mirror, {} mirror stream(s), {} left alone for a \
             dangling id",
            thousands(outcome.channels),
            thousands(outcome.planned_channels()),
            thousands(outcome.added_count()),
            thousands(outcome.skipped.len())
        ),
        writes,
        requests: requests_for(writes),
    }
}

fn visibility_phase(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    matching_config: &MatchingConfig,
    database: &ChannelDatabase,
) -> BenchPhase {
    let started = Instant::now();
    let options = MatchOptions::with_database(settings, matching_config, Some(database.clone()));
    let setup_time = started.elapsed();
    let started = Instant::now();
    let planned = visibility::plan_with_options(world, scope, &options, None);
    let plan_time = started.elapsed();
    let outcome = match planned {
        Ok(outcome) => outcome,
        Err(error) => return BenchPhase::failed("visibility", error),
    };

    let (writes, write_plan_time, error) = plan_write(
        world,
        scope,
        Field::Membership(outcome.profile_id),
        outcome.values(),
    );
    BenchPhase {
        action: "visibility".to_string(),
        setup_time: Some(setup_time),
        plan_time: Some(plan_time),
        write_plan_time,
        error,
        detail: format!(
            "profile {:?}: {} channel(s) in {} name group(s), {} winner(s), {} enabled before",
            outcome.profile_name,
            thousands(outcome.channels),
            thousands(outcome.groups),
            thousands(outcome.winners.len()),
            thousands(outcome.enabled_before)
        ),
        writes,
        requests: requests_for(writes),
    }
}

/// Time the four plans over one world. The database parse happens outside every timer: it is
/// a run's start-up cost, not a plan's, and it is the same for both modes.
fn phases(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    matching_config: &MatchingConfig,
    throughput: &dyn ThroughputSource,
) -> Vec<BenchPhase> {
    let database = database_codes(matching_config);
    vec![
        match_phase(
            world,
            scope,
            settings,
            matching_config,
            &database,
            throughput,
        ),
        sort_phase(world, scope, settings, matching_config, throughput),
        mirror_phase(world, scope),
        visibility_phase(world, scope, settings, matching_config, &database),
    ]
}

/// The notes every report carries: the build profile, and the settings that decide what a
/// plan writes.
fn common_notes(settings: &StreamsSettings, matching_config: &MatchingConfig) -> Vec<String> {
    let mut notes = Vec::new();
    if cfg!(debug_assertions) {
        notes.push(
            "DEBUG BUILD: these are debug numbers, 10-50x slower than a release build on this \
             code. Run `cargo run --release -p crosstalk -- bench streams ...` for the figures \
             a decision (or the README) should quote."
                .to_string(),
        );
    }
    notes.push(format!(
        "settings: overwrite_streams={}, keep_custom_streams={}, visible_channel_limit={}, \
         [matching] cut {}, throughput sorting {}",
        settings.overwrite_streams,
        settings.keep_custom_streams,
        settings.visible_channel_limit,
        matching_config.threshold(),
        if settings.enable_throughput_sorting {
            "on"
        } else {
            "off"
        }
    ));
    notes.push(format!(
        "requests are counted at {} rows per edit/bulk request, the writer's own chunk size; a \
         rejected chunk costs one PATCH per channel in it and is not predicted here",
        thousands(WRITE_CHUNK)
    ));
    notes
}

// =======================================================================================
// The two modes
// =======================================================================================

/// Time the four plans over a synthetic world. No network, no store, no Dispatcharr.
pub fn synthetic(
    settings: &StreamsSettings,
    matching_config: &MatchingConfig,
    options: &SyntheticOptions,
) -> Result<BenchReport> {
    let options = options.checked()?;
    let started = Instant::now();
    let world = synthetic_world(&options);
    let build = started.elapsed();

    // A synthetic world has one profile, "All", so a visibility plan can be timed: the action
    // needs a named profile, and every channel is in this one.
    let scope = ScopeConfig {
        profiles: "All".to_string(),
        ..ScopeConfig::default()
    };
    let throughput = MemoryThroughputSource::new();
    let phases = phases(&world, &scope, settings, matching_config, &throughput);

    let mut notes = common_notes(settings, matching_config);
    notes.push(
        "scope: every channel, in the synthetic world's only profile (\"All\"); [scope] in \
         crosstalk.toml is not applied to a synthetic world"
            .to_string(),
    );
    notes.push(
        "the synthetic world is built, not loaded: no Dispatcharr read is timed here, and \
         `--live` is what prices one"
            .to_string(),
    );
    Ok(BenchReport {
        title: options.describe(),
        setup: vec![("build the world".to_string(), build)],
        phases,
        write_timing: None,
        notes,
    })
}

/// Time the four plans against the Dispatcharr the configuration names, read-only.
///
/// `write_ids` is `--write-timing`'s channel sample: when it is `Some`, those channels' stream
/// lists are read back and rewritten to the value each holds at that moment, once per channel
/// and once as one bulk chunk, and both are extrapolated to [`WriteTiming::extrapolated`]
/// channels.
pub async fn live(
    config: &CrosstalkConfig,
    settings: &StreamsSettings,
    write_ids: Option<&[i64]>,
) -> Result<BenchReport> {
    let client = DispatcharrClient::new(&config.dispatcharr)
        .context("building the Dispatcharr client from crosstalk.toml")?;

    // Exactly the slices a match reads (EPG entries only when the placeholder path is on),
    // so the load time is the load a match pays.
    let mut kinds = WorldKinds::only(WorldKind::Channels)
        | WorldKind::Groups
        | WorldKind::Profiles
        | WorldKind::Streams
        | WorldKind::M3uAccounts;
    if settings.epg_placeholder_matching_enabled {
        kinds = kinds | WorldKind::EpgEntries;
    }
    let started = Instant::now();
    let world = client
        .load_world_with(kinds)
        .await
        .context("loading the world from Dispatcharr")?;
    let load = started.elapsed();

    // The store's `stream_health` is the throughput evidence a run reads; a machine without
    // one measures the same plans with every stream "unknown", which the report says.
    let store_path = config.data_dir().join("crosstalk.db");
    let throughput: Box<dyn ThroughputSource> = if store_path.exists() {
        let store = Store::open_shared(&store_path)
            .with_context(|| format!("opening {}", store_path.display()))?;
        Box::new(crate::quality::StoreThroughputSource::load(&store)?)
    } else {
        Box::new(MemoryThroughputSource::new())
    };
    let phases = phases(
        &world,
        &config.scope,
        settings,
        &config.matching,
        throughput.as_ref(),
    );

    let write_timing = match write_ids {
        Some(ids) => Some(timed_writes_in_a_run(&client, &world, ids, &store_path).await?),
        None => None,
    };

    let mut notes = common_notes(settings, &config.matching);
    notes.push(format!(
        "read-only: nothing was written by the plans; measured against {}",
        config.dispatcharr.api_root()
    ));
    if !store_path.exists() {
        notes.push(format!(
            "no run store at {}: every stream's throughput reads \"unknown\", as the plugin's \
             sort does with an empty cache",
            store_path.display()
        ));
    }
    Ok(BenchReport {
        title: format!(
            "live: {} channel(s), {} stream(s), {} account(s) from {}",
            thousands(world.channels.len()),
            thousands(world.streams.len()),
            thousands(world.m3u_accounts.len()),
            config.dispatcharr.api_root()
        ),
        setup: vec![("load_world_with".to_string(), load)],
        phases,
        write_timing,
        notes,
    })
}

/// The action id a `--write-timing` run is recorded under.
pub const ACTION_WRITE_TIMING: &str = "bench_write_timing";

/// [`time_writes`] inside a recorded streams run.
///
/// The timing WRITES stream lists, so it takes the streams app's one-live-run slot like any
/// other writer: a streams run in progress (here or in the daemon) refuses it instead of
/// racing it, and the run page records that it happened and how it went.
async fn timed_writes_in_a_run(
    client: &DispatcharrClient,
    world: &World,
    ids: &[i64],
    store_path: &std::path::Path,
) -> Result<WriteTiming> {
    let store = Store::open_shared(store_path)
        .with_context(|| format!("opening {}", store_path.display()))?;
    let run_id = store
        .start_run(crate::APP_ID, ACTION_WRITE_TIMING, false)
        .context(
            "--write-timing rewrites stream lists, so it needs the streams app's run slot, and \
             a streams run is already in progress (here or in another process): wait for it to \
             finish",
        )?;
    let timed = time_writes(client, world, ids).await;
    let (status, summary) = match &timed {
        Ok(timing) => (
            RunStatus::Succeeded,
            format!(
                "write timing over {} channel(s): {} failed write(s), each list rewritten to the \
                 value read back just before",
                timing.sample, timing.failed
            ),
        ),
        Err(error) => (RunStatus::Failed, format!("{error:#}")),
    };
    store.finish_run(run_id, status, Some(&summary), &serde_json::json!({}), None)?;
    timed.context("timing stream-list writes")
}

/// `--write-timing`: the two request patterns, measured on the same lists.
///
/// The write is an idempotent no-op — each channel gets the list just read back — so a
/// failed or partial run leaves the install exactly as it was. The per-channel path runs
/// first because it is the one that warms nothing: the bulk request that follows pays the
/// same connection setup the writer would.
async fn time_writes(
    client: &DispatcharrClient,
    world: &World,
    ids: &[i64],
) -> Result<WriteTiming> {
    if ids.is_empty() {
        bail!("--write-timing needs --channel-ids: name the channels to sample");
    }
    // A typo otherwise becomes N failed writes instead of one clear refusal: the ids must
    // name channels the world load saw.
    let missing: Vec<String> = ids
        .iter()
        .filter(|id| !world.channels.iter().any(|channel| channel.id == **id))
        .map(|id| id.to_string())
        .collect();
    if !missing.is_empty() {
        bail!(
            "channel id(s) {} are not in the channel list Dispatcharr answered; --channel-ids \
             must name channels that exist",
            missing.join(", ")
        );
    }

    // The lists are read back AFTER the four plans ran, one GET per channel, immediately
    // before the writes — never from the world above. That world was loaded before the
    // plans, so its lists are minutes stale on a busy box; rewriting them would revert a
    // concurrent edit instead of being the no-op this timing promises. They come from the
    // channel's own row (its `streams` ids, exactly the list the write replaces), not from a
    // stream listing, whose filters could leave a stream out and so unlink it.
    let mut lists: Vec<(i64, Vec<i64>)> = Vec::with_capacity(ids.len());
    for id in ids {
        let streams = client.channel_stream_ids(*id).await.with_context(|| {
            format!(
                "reading the current stream list of channel {id} immediately before the \
                 write timing"
            )
        })?;
        lists.push((*id, streams));
    }

    let mut failed = 0usize;
    let started = Instant::now();
    for (id, streams) in &lists {
        if client.set_channel_streams(*id, streams).await.is_err() {
            failed += 1;
        }
    }
    let per_channel = started.elapsed();

    let started = Instant::now();
    let results = client.bulk_set_channel_streams(&lists).await;
    let bulk = started.elapsed();
    failed += results.iter().filter(|(_, result)| result.is_err()).count();

    Ok(WriteTiming {
        sample: lists.len(),
        per_channel,
        bulk,
        failed,
        // The plan's target size. A live install of another size still gets the budget rule
        // applied at the size the plan was written for, which is what the README claims.
        extrapolated: SyntheticOptions::default().channels,
    })
}

// =======================================================================================
// Formatting
// =======================================================================================

pub fn seconds(elapsed: Option<Duration>) -> String {
    match elapsed {
        Some(elapsed) => format!("{:.3}s", elapsed.as_secs_f64()),
        None => "-".to_string(),
    }
}

/// `20000` as `20,000`: a six-figure count with no separator is one digit away from a
/// misreading, and these numbers are quoted.
pub fn thousands(count: usize) -> String {
    let digits = count.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> StreamsSettings {
        StreamsSettings::default()
    }

    fn matching() -> MatchingConfig {
        CrosstalkConfig::default().matching
    }

    fn small() -> SyntheticOptions {
        SyntheticOptions {
            channels: 30,
            streams: 200,
            accounts: 20,
        }
    }

    #[test]
    fn a_synthetic_world_has_the_shape_the_options_ask_for() {
        let options = small();
        let world = synthetic_world(&options);
        assert_eq!(world.channels.len(), options.channels);
        assert_eq!(world.streams.len(), options.streams);
        assert_eq!(world.m3u_accounts.len(), options.accounts);
        assert_eq!(world.profiles.len(), 1, "visibility needs a profile");
        assert_eq!(
            world.profiles[0].channels.len(),
            options.channels,
            "every channel is in the one profile"
        );
        for channel in &world.channels {
            assert_eq!(
                channel.streams.len(),
                STREAMS_PER_CHANNEL,
                "channel {} carries ten streams",
                channel.id
            );
        }
        // Every stream id a channel holds resolves to a stream in the world.
        let ids: std::collections::HashSet<i64> =
            world.streams.iter().map(|stream| stream.id).collect();
        for channel in &world.channels {
            for id in &channel.streams {
                assert!(
                    ids.contains(id),
                    "channel {} holds dangling {id}",
                    channel.id
                );
            }
        }
        // Two hosts per feed, under one provider id: what a mirror fill copies.
        let mut by_provider: HashMap<i64, usize> = HashMap::new();
        for stream in &world.streams {
            *by_provider
                .entry(stream.stream_id.expect("a provider id"))
                .or_default() += 1;
        }
        assert!(
            by_provider.values().all(|count| *count == 2),
            "every provider stream id is carried by two accounts: {by_provider:?}"
        );
        assert!(
            world.m3u_accounts.iter().all(|account| account
                .server_url
                .as_deref()
                .is_some_and(|url| url.contains("://"))),
            "every account needs a parseable server_url, or a mirror fill has no host to \
             compare"
        );
    }

    #[test]
    fn the_synthetic_world_is_the_same_every_run() {
        let first = synthetic_world(&small());
        let second = synthetic_world(&small());
        assert_eq!(first.channels.len(), second.channels.len());
        assert_eq!(
            first.streams.iter().map(|s| s.id).collect::<Vec<_>>(),
            second.streams.iter().map(|s| s.id).collect::<Vec<_>>()
        );
        assert_eq!(
            first
                .channels
                .iter()
                .map(|c| c.streams.clone())
                .collect::<Vec<_>>(),
            second
                .channels
                .iter()
                .map(|c| c.streams.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn requests_are_counted_in_writer_chunks() {
        assert_eq!(WRITE_CHUNK, 500);
        assert_eq!(requests_for(0), 0);
        assert_eq!(requests_for(1), 1);
        assert_eq!(requests_for(WRITE_CHUNK), 1);
        assert_eq!(requests_for(WRITE_CHUNK + 1), 2);
        assert_eq!(requests_for(20_000), 40);
    }

    #[test]
    fn a_synthetic_bench_times_every_phase_and_counts_its_writes() {
        let report = synthetic(&settings(), &matching(), &small()).expect("runs");
        assert_eq!(report.phases.len(), 4);
        let actions: Vec<&str> = report
            .phases
            .iter()
            .map(|phase| phase.action.as_str())
            .collect();
        assert_eq!(actions, vec!["match", "sort", "mirror", "visibility"]);
        for phase in &report.phases {
            assert!(
                phase.error.is_none(),
                "{} failed: {:?}",
                phase.action,
                phase.error
            );
            assert!(phase.plan_time.is_some(), "{}", phase.action);
            assert!(phase.write_plan_time.is_some(), "{}", phase.action);
            assert_eq!(
                phase.requests,
                requests_for(phase.writes),
                "{} requests follow its writes",
                phase.action
            );
            assert!(
                phase.writes > 0,
                "{} would write nothing, so the world does not exercise it",
                phase.action
            );
            assert!(
                phase.writes <= small().channels,
                "{} writes more channels than exist",
                phase.action
            );
        }
        // Sort, match and mirror are about stream lists; visibility is about membership.
        assert!(report.write_timing.is_none(), "synthetic mode never writes");
        // A title, the setup line and one line per phase, at least.
        assert!(report.lines().len() > report.phases.len());
    }

    #[test]
    fn the_report_says_so_when_the_build_is_debug() {
        let report = synthetic(&settings(), &matching(), &small()).expect("runs");
        let debug_note = report
            .notes
            .iter()
            .any(|note| note.starts_with("DEBUG BUILD"));
        assert_eq!(
            debug_note,
            cfg!(debug_assertions),
            "the note is present exactly when the build is a debug one"
        );
        // The settings note is always there: the writes it reports depend on them.
        assert!(report
            .notes
            .iter()
            .any(|note| note.starts_with("settings: overwrite_streams=")));
    }

    #[test]
    fn a_zero_sized_world_is_refused() {
        let error = synthetic(
            &settings(),
            &matching(),
            &SyntheticOptions {
                channels: 0,
                streams: 10,
                accounts: 2,
            },
        )
        .expect_err("refused");
        assert!(
            format!("{error}").contains("at least one channel"),
            "{error}"
        );
    }

    #[test]
    fn the_recommendation_applies_the_budget_rule() {
        let fast = WriteTiming {
            sample: 10,
            per_channel: Duration::from_millis(20),
            bulk: Duration::from_micros(500),
            failed: 0,
            extrapolated: 20_000,
        };
        assert!(
            fast.recommendation().contains("REST is enough"),
            "{}",
            fast.recommendation()
        );

        let slow = WriteTiming {
            sample: 10,
            per_channel: Duration::from_millis(2_000),
            bulk: Duration::from_secs(1),
            failed: 0,
            extrapolated: 20_000,
        };
        // 1 s over 10 channels is 2000 s over 20,000: past the 120 s budget.
        assert!(
            slow.recommendation().contains("over the budget"),
            "{}",
            slow.recommendation()
        );

        let broken = WriteTiming {
            failed: 2,
            ..fast.clone()
        };
        assert!(
            broken.recommendation().contains("refused"),
            "{}",
            broken.recommendation()
        );
    }

    #[test]
    fn seconds_and_counts_read_the_way_a_report_needs() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(20_000), "20,000");
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(seconds(Some(Duration::from_millis(1234))), "1.234s");
        assert_eq!(seconds(None), "-");
    }

    #[tokio::test]
    async fn a_live_bench_times_the_plans_and_the_two_write_patterns() {
        use crate::test_support::{sort_world, ServerFixture, CHANNEL_SD_FIRST, CHANNEL_SORTED};
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, ResponseTemplate};

        let world = sort_world();
        let fixture = ServerFixture::start(&world).await;
        // The mock set covers the bulk write; the per-channel PATCH is what the writer falls
        // back to, so the bench's per-channel sample needs it too.
        Mock::given(method("PATCH"))
            .and(path_regex(r"^/api/channels/channels/\d+/$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 1})))
            .mount(&fixture.server)
            .await;
        // `--write-timing` reads each sampled channel's list back immediately before the
        // writes (never from the loaded world), from the channel's own row, so the mock
        // serves those reads from the same world the load saw.
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/channels/channels/\d+/$"))
            .respond_with(move |request: &wiremock::Request| {
                let id: i64 = request
                    .url
                    .path()
                    .split('/')
                    .nth(4)
                    .and_then(|id| id.parse().ok())
                    .unwrap_or(-1);
                match world.channels.iter().find(|channel| channel.id == id) {
                    Some(channel) => ResponseTemplate::new(200)
                        .set_body_json(serde_json::to_value(channel).expect("channel")),
                    None => ResponseTemplate::new(404),
                }
            })
            .mount(&fixture.server)
            .await;

        let sampled = [CHANNEL_SD_FIRST, CHANNEL_SORTED];
        let report = live(&fixture.config, &StreamsSettings::default(), Some(&sampled))
            .await
            .expect("the mocked install answers every read");

        // The world load is timed on its own, and the four plans are all reported — the
        // visibility one as "not measurable", because [scope] profiles is empty.
        assert!(report
            .setup
            .iter()
            .any(|(label, _)| label == "load_world_with"));
        assert_eq!(report.phases.len(), 4);
        let visibility = report
            .phases
            .iter()
            .find(|phase| phase.action == "visibility")
            .expect("a visibility phase");
        assert!(
            visibility
                .error
                .as_deref()
                .is_some_and(|error| error.contains("Profile")),
            "{:?}",
            visibility.error
        );
        for phase in report
            .phases
            .iter()
            .filter(|phase| phase.action != "visibility")
        {
            assert!(phase.error.is_none(), "{}: {:?}", phase.action, phase.error);
        }

        // Both request patterns ran, and nothing was refused: one PATCH per sampled channel
        // and one bulk request carrying both lists.
        let timing = report.write_timing.expect("write timing");
        assert_eq!(timing.sample, sampled.len());
        assert_eq!(timing.failed, 0, "{}", timing.recommendation());
        // The verdict depends on wall-clock timings extrapolated to 20k channels, so a busy
        // machine can tip it either way; the budget rule itself has its own pure test. Here it
        // only has to reach one of the two verdicts.
        let verdict = timing.recommendation();
        assert!(
            verdict.contains("REST is enough") || verdict.contains("bridge"),
            "{verdict}"
        );
        let per_channel = fixture
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() == "PATCH")
            .filter(|request| {
                request.url.path().starts_with("/api/channels/channels/")
                    && !request.url.path().ends_with("/edit/bulk/")
            })
            .count();
        assert_eq!(per_channel, sampled.len(), "one PATCH per sampled channel");
        let bulk = fixture
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.url.path() == "/api/channels/channels/edit/bulk/")
            .count();
        assert_eq!(bulk, 1, "one edit/bulk request for the same lists");
        // The lists the writes carried were read back just before, not taken from the
        // loaded world: one list read per sampled channel.
        let rereads = fixture
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| {
                let path = request.url.path();
                request.method.as_str() == "GET"
                    && path.starts_with("/api/channels/channels/")
                    && path != "/api/channels/channels/"
            })
            .count();
        assert_eq!(
            rereads,
            sampled.len(),
            "one fresh list read per sampled channel"
        );
    }

    #[tokio::test]
    async fn write_timing_waits_for_the_streams_slot() {
        // S10: the timing writes stream lists, so a streams run in progress (here, or a
        // daemon's) refuses it instead of racing it.
        use crate::test_support::{sort_world, ServerFixture, CHANNEL_SORTED};

        let fixture = ServerFixture::start(&sort_world()).await;
        let store =
            Store::open_shared(fixture.config.data_dir().join("crosstalk.db")).expect("store");
        let running = store
            .start_run(crate::APP_ID, "sort_apply", false)
            .expect("a live streams run");
        let error = live(
            &fixture.config,
            &StreamsSettings::default(),
            Some(&[CHANNEL_SORTED]),
        )
        .await
        .expect_err("refused");
        assert!(format!("{error:#}").contains("run slot"), "{error:#}");
        let writes = fixture
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() != "GET")
            .count();
        assert_eq!(writes, 0, "nothing was written");
        assert_eq!(
            store.run(running).expect("run").expect("present").status,
            RunStatus::Running
        );
    }

    #[tokio::test]
    async fn a_live_bench_refuses_a_channel_that_does_not_exist() {
        use crate::test_support::{sort_world, ServerFixture};

        let fixture = ServerFixture::start(&sort_world()).await;
        let error = live(&fixture.config, &StreamsSettings::default(), Some(&[9_999]))
            .await
            .expect_err("refused");
        assert!(format!("{error:#}").contains("9999"), "{error:#}");
    }

    #[tokio::test]
    async fn a_live_bench_without_a_store_says_throughput_is_unknown() {
        use crate::test_support::{sort_world, ServerFixture};

        let fixture = ServerFixture::start(&sort_world()).await;
        let report = live(&fixture.config, &StreamsSettings::default(), None)
            .await
            .expect("runs");
        assert!(report.write_timing.is_none(), "read-only by default");
        assert!(
            report.notes.iter().any(|note| note.contains("read-only")),
            "{:?}",
            report.notes
        );
    }
}
