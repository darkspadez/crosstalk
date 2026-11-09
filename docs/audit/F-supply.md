# Area F report (supply chain / deployment). cargo audit (orchestrator run): 0 vulns, 1 warning basic-toml unmaintained (via askama 0.12/askama_axum 0.4).

## Status (fix 6, branch audit/fix-f)

| Finding | Status | What changed / what is left |
|---|---|---|
| F-B1 EOL Alpine 3.20 | FIXED | Dockerfile runtime `alpine:3.22@sha256:4934…898f` (3.22.8), builder `rust:1.99.1-alpine3.22@sha256:9a74…9ff0`; rust-toolchain.toml no longer copied into the builder (rustup would otherwise download today's stable and bypass the pin). ffmpeg via apk, unpinned on purpose (comment). Renovate `docker:pinDigests` keeps tag+digest moving. |
| F-B2 shared cache volume | FIXED | ci.yml `check`/`parity` and build.yml `binaries`: PRs use `nscloud-cache-tag-crosstalk-pr-<arch>`; build.yml on `refs/tags/*` runs on a runner without `-with-cache`/cache-tag and skips nscloud-cache-action; the release image build sets `no-cache`. PR image builds still cache-only. Repo setting (approval for all outside contributors) is the orchestrator's. |
| F-S1 ci.yml permissions | FIXED | Top-level `permissions: contents: read`; `persist-credentials: false` on all 9 checkouts (submodules are public https). |
| F-S2 movable action refs | FIXED | Every `uses:` pinned to a full SHA with `# vX.Y.Z` (dtolnay/rust-toolchain: `stable` branch head, `toolchain: stable` explicit). The grep is empty. |
| F-S3 releases skip CI | FIXED | ci.yml runs on `v*` tags; build.yml `ci-gate` (tags only, `actions: read`) polls the CI run for this tag/SHA/push event, up to 60 min; `image` and `release` need it. PR/main builds unchanged. README "Releases" section. Tag ruleset for `v*` still to set (repo setting). |
| F-S4 no provenance/SBOM | FIXED | build-push `provenance: mode=max`, `sbom: true` (pushes only); `actions/attest-build-provenance` on the image digest (push-to-registry) and `dist/*.tar.gz`; `id-token`/`attestations: write` only on `image` and `release`. README has `gh attestation verify`. |
| F-S5 licences | FIXED (needs a run) | `about.toml` + `about.hbs` -> `THIRD_PARTY_LICENSES` (build.yml `licenses` job, into tarballs and `/usr/share/doc/crosstalk/`); THIRD_PARTY_NOTICES.md: Rust crates + Alpine ffmpeg 6.1.2-r2 (GPL build, aports URL); `deny.toml` + `cargo deny --locked check licenses advisories bans` in the audit job; audit.toml `[output] deny = ["unsound","yanked"]` (`unmaintained` once basic-toml is gone, see F-S8). |
| F-S6 compose 0.0.0.0 | FIXED | `"127.0.0.1:8730:8730"` with a reverse-proxy comment. |
| F-S7 container privileges | FIXED | compose `read_only`, `tmpfs: [/tmp]`, `cap_drop: [ALL]`, `no-new-privileges`; same flags in the CI smoke runs; image `WORKDIR /tmp` (child-process side files land on the tmpfs) and `ENV CROSSTALK_CONFIG_DIR=/config` (the CLI's relative default). |
| F-S8 askama / basic-toml | PARTIAL | askama 0.16 (commit e159777) dropped `askama_axum`, but `askama_derive` 0.16.1 still reads its config through `basic-toml` (`cargo tree -i basic-toml`), so RUSTSEC-2026-0374 stays ignored in deny.toml and `unmaintained` stays out of audit.toml's deny list until askama's derive crate drops it. Build-time only (a proc-macro dependency): it parses `askama.toml` in this repository, never runtime input. |
| F-N1 toolchain/image/apk pins | PARTIAL | Image tags and digests pinned; rust-toolchain.toml stays `stable` (user decision); apk versions unpinned (Alpine drops old versions). |
| F-N2 MSRV untested | FIXED (needs a run) | The declared 1.82 was false: the lockfile needs 1.89 (uuid 1.29; icu 2.4 needs 1.88; clap/tokio/indexmap 1.85, edition 2024). `rust-version = "1.89"` and a CI `msrv` job (`cargo +1.89 check --workspace --locked`). |
| F-N3 python without -I | FIXED | ci.yml parity_sweep runs, sim_parity.sh (3 calls), oracle_report_guide.py's subprocess and README; parity_sweep.py refuses to run without `-I`. |
| F-N4 drift step unreachable | FIXED | ci.yml has `workflow_dispatch`. |
| F-N5 slim ffmpeg | OPEN | Later. |
| F-N6 `*_FILE` secrets | OPEN | Not in this fix. |
| F-N7 duplicate versions | FIXED | cargo-deny `multiple-versions = "warn"`; lockFileMaintenance weekly. |
| F-N8 unsafe / killpg | PARTIAL | `#![forbid(unsafe_code)]` on crosstalk, crosstalk-core, crosstalk-matching, crosstalk-web, guide, lineup (none had unsafe). streams (the killpg) not touched here: another fixer owns it. |
| F-N9 fragile version check | FIXED | `cargo metadata --no-deps | jq` for the `crosstalk` package. |
| Renovate | FIXED | `renovate.json` (config:recommended, pinGitHubActionDigests, docker:pinDigests, semantic commits off, weekly lockFileMaintenance, vulnerabilityAlerts, git-submodules disabled, Cargo minor+patch grouped). PR #1 can be closed. |

## Findings (original report)

F-B1 BLOCKER runtime image alpine:3.20 EOL (Dockerfile:19, :32); ffmpeg decodes untrusted streams w/o CVE fixes. Fix: alpine:3.22 (or newer) pinned by digest; Renovate docker manager. Verify: cat /etc/alpine-release.
F-B2 BLOCKER PR and release builds share one Namespace cache volume (ci.yml:18-20,31; :114-116,128; build.yml:56-58,71 tag nscloud-cache-tag-crosstalk-<arch>) -> fork PR code (build.rs/tests/proc-macros) can poison ~/.cargo/registry/src or target/ reused by v* tag builds. Fix: separate cache tag for pull_request events (crosstalk-pr-<arch>) AND release builds without cache (drop -with-cache label or cargo clean + fresh CARGO_HOME); repo setting "require approval for all outside collaborators".
F-S1 ci.yml has no permissions: block -> top-level permissions: contents: read; persist-credentials: false on every checkout (both workflows).
F-S2 actions pinned by movable tags/branch (ci.yml:22,25,31,59,103,104,123,127,128,131; build.yml:33,60,67,71,96,116,124,138,157; dtolnay/rust-toolchain@stable) -> pin every uses: to full SHA with # vX.Y.Z comment; Renovate helpers:pinGitHubActionDigests. Verify grep -nE 'uses: [^@]+@(v[0-9]+|stable|main)$' empty.
F-S3 release doesn't wait for CI (build.yml:9-14,43-46,102-104,150-152) -> ci.yml push tags v*; build.yml release/image jobs gated (workflow_run on CI success, or move release into ci.yml behind needs). Tag ruleset for v*.
F-S4 no signing/provenance/SBOM (build.yml:137-148,163-174) -> provenance mode=max, sbom: true, actions/attest-build-provenance on image digest and dist/*.tar.gz (id-token: write, attestations: write on those jobs only).
F-S5 THIRD_PARTY_NOTICES.md lacks Rust crate licences + Alpine ffmpeg notice -> cargo about generate (or cargo-bundle-licenses) in build.yml -> THIRD_PARTY_LICENSES in tarball + /usr/share/doc/crosstalk/; paragraph on ffmpeg package/licence/aports; cargo deny check licenses in CI.
F-S6 compose publishes 0.0.0.0:8730 (docker-compose.example.yml:34) -> "127.0.0.1:8730:8730" with comment.
F-S7 container default privileges -> compose: read_only: true, tmpfs [/tmp], cap_drop [ALL], security_opt no-new-privileges; same flags in CI docker smoke (ci.yml:69-73); ensure daemon cwd writes (ffprobe -report, D1) don't break.
F-S8 askama 0.12/askama_axum 0.4 pull unmaintained basic-toml -> upgrade askama (IntoResponse over Template::render), audit.toml [output] deny = ["unmaintained","unsound","yanked"].
NITs F-N1 pin rust toolchain version + image digests, apk versions; F-N2 rust-version 1.82 never tested (job or drop); F-N3 python without -I: ci.yml:150-151 parity_sweep.py, scripts/sim_parity.sh:149 -> add -I and sys.flags.isolated guard; F-N4 upstream_drift workflow_dispatch condition never fires (add trigger); F-N5 slim ffmpeg (later); F-N6 support *_FILE secrets; F-N7 duplicate crate versions (cargo deny bans warn); F-N8 unsafe killpg sound; normal-exit path doesn't killpg and collect unbounded (tool.rs:280-282, 320-323) -> always killpg after exit; #![forbid(unsafe_code)] on every crate except streams; F-N9 version check fragile (build.yml:34-41) -> cargo metadata | jq; prerelease tags can't match.
Renovate PR #1 (branch origin/renovate/configure) unread: recommend helpers:pinGitHubActionDigests, docker:pinDigests, lockFileMaintenance weekly, vulnerabilityAlerts, git-submodules disabled explicitly.
