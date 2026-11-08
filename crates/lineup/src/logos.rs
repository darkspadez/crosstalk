//! `logos_*`: Channel-Mapparr's and Lineuparr's logo assignment on the unified resolver.
//!
//! Port of `reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/plugin.py`
//! `apply_logos_action` (~2383), `apply_tv_logos_action` / `replace_tv_logos_action`
//! / `_assign_tv_logos` (~2492) and
//! `reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/plugin.py`
//! `_do_apply_logo_match` (~3170): the tier order, the station-first tv-logos
//! match, the fuzzy filename match, the exact-name Logo Manager hit, the default
//! logo and the replace/apply distinction.
//!
//! # Resolution algorithm per tier (with citations)
//!
//! Per in-scope channel ([`decide_logo`]), through the ONE resolver
//! ([`logos::resolve_logo`] in `core::logos`: tiers, order, country search and
//! default safety net live there), consulted in the configured `logo_sources`
//! order ([`LogoSource::parse_order`], default `epg, logo_manager, tv-logos,
//! network` — Lineuparr's tier order, which the goal reset decided as the unified
//! one, plus CM's network fallback last, where CM consults it):
//!
//! 1. **EPG icon**: the icon URL of the channel's assigned EPG entry (`Lineuparr
//!    plugin.py:3230`, Tier 1; CM's `_epg_icon_urls`, `plugin.py:2787`, only for
//!    channels with no logo — here it is a tier like any other, so `replace_*`
//!    consults it too).
//! 2. **Logo Manager**: a logo whose NAME matches the channel name exactly,
//!    case-insensitively (`existing_logos_by_name`, `Lineuparr plugin.py:3243`).
//! 3. **tv-logos** (`logo_matcher.py` both plugins): a station-format name
//!    (`parse_station_name`) takes the station file first — its own directory
//!    (`us-local`), then the country directory (CM's two-phase order across
//!    every searched country) — else the best fuzzy filename match at threshold
//!    85 ([`match_channel_to_logo`](crosstalk_core::logos::match_channel_to_logo)).
//!    Which countries are searched: `tv_logos_countries` when set, else the
//!    identity's country, else the enabled `[matching]` countries. CM searched
//!    its `channel_databases` list; LU searched its single lineup country.
//! 4. **Network** (CM's `network_logo`, `plugin.py:2669-2683`): a station-format
//!    name with no station file takes its network's logo
//!    (`<network>-<suffix>.png`, else the white variant) from the searched
//!    countries — only for a channel with NO logo, like CM, so a replace run
//!    never swaps a logo for the generic network one.
//! 5. **Default**: `default_logo`, a Logo Manager entry by exact
//!    case-insensitive name (CM's `apply_logos_action`, which errors when the
//!    name is not in the manager). Here a missing default is a per-channel skip
//!    with the reason, not a whole-run error: one run assigns many channels,
//!    and a missing default must not blank the channels a better tier found.
//!
//! The default tier is consulted after the listed tiers even when `logo_sources`
//! does not name it (it is the safety net, like CM's separate apply action);
//! naming `default` in `logo_sources` places it explicitly. The threshold stays
//! 85 in both modes: CM's replace mode followed match sensitivity, but the
//! logo similarity is the unified engine's, not a sensitivity decision, so one
//! fixed threshold serves both — the replace/apply difference is only whether a
//! channel that already has a logo is considered at all.
//!
//! The tv-logos match runs on the channel name without its leading country
//! marker ([`tv_match_name`]: `US: CNN HD` -> `CNN HD`; the logo normalizer
//! already drops trailing quality tags), so a decorated name and its bare form
//! take the same file. Not the identity's alias canonical: canonicals are DB
//! spellings (`HISTORY Channel, The`, `Paramount+ with SHOWTIME EAST`) that miss
//! the right file or reach a wrong one (step 16 review, oracle-checked).
//!
//! # Planning, CSV, undo
//!
//! `logos_preview` (Read) / `logos_apply` (Write, confirm) plan from LIVE
//! state. A channel with a logo is skipped unless `replace_existing` (CM's
//! `has_no_logo`: `None` or `0`; the `"0"` string form only exists in ORM
//! rows). A resolved URL already in the world reuses that logo's id; any other
//! URL becomes a create-stage [`NewLogo`] (deduped by exact URL across the run,
//! owned by lineup under [`logo_owner_key`]), and the assignment is an
//! [`Op::Set`] `Logo` per channel. Apply creates logos first, then re-plans the
//! assignments against the world augmented with the created logos and writes
//! through [`channel_ops::apply`]. CM's replace mode wrote a JSON backup of the
//! previous logos first; crosstalk records the before values in the run rows
//! instead, and the host [`undo`](crosstalk_core::undo) reverses them (created
//! logos are left and listed, like created groups).
//!
//! Collision and number checks are n/a: logos are looked up by URL and assigned
//! by id; nothing is renamed or numbered.

use anyhow::Result;
use crosstalk_core::channel_ops::{
    self, ChannelCreator, CreateRequest, Field, NewLogo, Op, PlanOptions, RestChannelCreator,
    Selection, SetValue,
};
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::{World, WorldKind, WorldKinds};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::fetch_cache::FetchCache;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::logos::{
    self, LogoQuery, LogoSource, LogoSources, TvHit, TvLogosIndex, LOGO_MATCH_THRESHOLD,
    TV_LOGOS_TREE_URL,
};
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_matching::identity::{Identity, ResolveContext};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The CSV columns: the per-channel logo decision plus the identity fields.
pub const COLUMNS: [&str; 12] = [
    "id",
    "channel_number",
    "group",
    "channel_name",
    "old_logo",
    "new_logo",
    "source",
    "status",
    "reason",
    "callsign",
    "country",
    "canonical",
];

/// Parse `tv_logos_countries` into uppercase ISO-2 codes, order kept, deduped.
pub fn parse_country_codes(raw: &str) -> Vec<String> {
    let mut codes: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for token in raw.split([',', '\n']) {
        let code = token.trim().to_uppercase();
        if code.is_empty() || !seen.insert(code.clone()) {
            continue;
        }
        codes.push(code);
    }
    codes
}

/// The name the tv-logos tier matches: the channel name without its leading
/// delimited country marker (`US: CNN HD` -> `CNN HD`), the one decoration the
/// logo normalizer does not drop.
pub fn tv_match_name(name: &str) -> &str {
    crosstalk_matching::country::strip_delimited_country_marker(name)
}

/// The ownership key of a logo lineup creates: `lineup:logo:<url>`.
pub fn logo_owner_key(url: &str) -> String {
    format!("lineup:logo:{url}")
}

/// One in-scope channel, as logos sees it.
#[derive(Debug, Clone)]
pub struct ScopedChannel {
    pub id: i64,
    pub name: String,
    pub number: Option<f64>,
    pub group: String,
    pub logo_id: Option<i64>,
    pub epg_id: Option<i64>,
}

/// The countries one channel's tv-logos tier searches: the
/// `tv_logos_countries` override, else the identity's country, else the enabled
/// `[matching]` countries (CM's `channel_databases` list; LU's single lineup
/// country is the one-code special case).
pub fn tv_countries(
    settings: &crate::settings::LineupSettings,
    identity: &Identity,
    enabled: &[String],
) -> Vec<String> {
    let override_codes = parse_country_codes(&settings.tv_logos_countries);
    if !override_codes.is_empty() {
        return override_codes;
    }
    if let Some(country) = identity.country.as_deref() {
        let code = country.trim().to_uppercase();
        if !code.is_empty() {
            return vec![code];
        }
    }
    enabled
        .iter()
        .map(|code| code.trim().to_uppercase())
        .filter(|code| !code.is_empty())
        .collect()
}

/// One channel's logo decision: the URL to assign (if any) and how it was found.
#[derive(Debug, Clone)]
pub struct LogoRow {
    pub id: i64,
    pub group: String,
    pub number: Option<f64>,
    pub old: String,
    /// The current logo's name, when the world names it.
    pub current_logo: Option<String>,
    /// The URL to assign, when a tier hit.
    pub url: Option<String>,
    pub source: Option<LogoSource>,
    /// The world id of the URL, when it is already a Logo row.
    pub existing_logo_id: Option<i64>,
    /// The tv-logos hit detail (file, score), for the oracle dump.
    pub tv_hit: Option<TvHit>,
    pub reason: String,
    pub identity: Identity,
}

/// What the world offers the tiers: logos by lowercase name and by exact URL,
/// EPG entry icons by id.
pub struct LogoWorld<'a> {
    pub by_name: HashMap<String, (i64, String)>,
    pub by_url: HashMap<&'a str, (i64, String)>,
    pub by_id: HashMap<i64, (&'a str, &'a str)>,
    pub epg_icons: HashMap<i64, &'a str>,
}

impl<'a> LogoWorld<'a> {
    pub fn of(world: &'a World) -> Self {
        let mut by_name: HashMap<String, (i64, String)> = HashMap::new();
        let mut by_url: HashMap<&str, (i64, String)> = HashMap::new();
        let mut by_id: HashMap<i64, (&str, &str)> = HashMap::new();
        for logo in &world.logos {
            by_name
                .entry(logo.name.to_lowercase())
                .or_insert((logo.id, logo.url.clone()));
            by_url
                .entry(logo.url.as_str())
                .or_insert((logo.id, logo.name.clone()));
            by_id
                .entry(logo.id)
                .or_insert((logo.name.as_str(), logo.url.as_str()));
        }
        let mut epg_icons: HashMap<i64, &str> = HashMap::new();
        for entry in &world.entries {
            if let Some(icon) = entry.icon_url.as_deref() {
                if !icon.trim().is_empty() {
                    epg_icons.insert(entry.id, icon);
                }
            }
        }
        Self {
            by_name,
            by_url,
            by_id,
            epg_icons,
        }
    }

    /// The run's resolver inputs for [`logos::resolve_logo`]: the consult order, the
    /// Logo Manager's names, the tree, and the default logo — a Logo Manager entry by
    /// exact case-insensitive name (CM's `apply_logos_action`), `None` when the
    /// manager does not hold it.
    pub fn sources<'t>(
        &self,
        order: Vec<LogoSource>,
        tv_logos: Option<&'t TvLogosIndex>,
        settings: &crate::settings::LineupSettings,
    ) -> LogoSources<'t> {
        let default = settings.default_logo.trim();
        LogoSources {
            order,
            manager_by_name: self
                .by_name
                .iter()
                .map(|(name, (_, url))| (name.clone(), url.clone()))
                .collect(),
            tv_logos,
            default_logo: (!default.is_empty())
                .then(|| self.by_name.get(&default.to_lowercase()))
                .flatten()
                .map(|(_, url)| url.clone()),
            threshold: Some(LOGO_MATCH_THRESHOLD),
        }
    }
}

/// Decide one channel's logo through the unified resolver ([`logos::resolve_logo`]:
/// the one tier walk, order and country search).
///
/// A channel with a logo is skipped unless `replace_existing`. The tv-logos
/// tier matches on [`tv_match_name`]; every other tier is exact. A
/// configured default the Logo Manager does not hold is the skip reason when no
/// tier resolves (a per-channel skip, not a run error).
pub fn decide_logo(
    channel: &ScopedChannel,
    identity: &Identity,
    lookups: &LogoWorld<'_>,
    sources: &LogoSources<'_>,
    countries: &[String],
    settings: &crate::settings::LineupSettings,
) -> LogoRow {
    let row = |url: Option<String>,
               source: Option<LogoSource>,
               existing_logo_id: Option<i64>,
               tv_hit: Option<TvHit>,
               reason: String| LogoRow {
        id: channel.id,
        group: channel.group.clone(),
        number: channel.number,
        old: channel.name.clone(),
        current_logo: channel_logo_name(channel, lookups),
        url,
        source,
        existing_logo_id,
        tv_hit,
        reason,
        identity: identity.clone(),
    };
    if has_logo_id(channel.logo_id) && !settings.replace_existing {
        return row(
            None,
            None,
            None,
            None,
            "the channel already has a logo and replace_existing is off".to_string(),
        );
    }
    let query = LogoQuery {
        name: &channel.name,
        match_name: tv_match_name(&channel.name),
        epg_icon: channel
            .epg_id
            .and_then(|id| lookups.epg_icons.get(&id).copied()),
        countries,
        has_logo: has_logo_id(channel.logo_id),
    };
    let Some(resolved) = logos::resolve_logo(&query, sources) else {
        let name = settings.default_logo.trim();
        let reason = if !name.is_empty() && sources.default_logo.is_none() {
            format!("the default logo {name:?} is not in the Logo Manager")
        } else {
            "no tier matched".to_string()
        };
        return row(None, None, None, None, reason);
    };
    let reason = match (resolved.source, resolved.tv_hit.as_ref()) {
        (LogoSource::EpgIcon, _) => "the assigned EPG entry's icon".to_string(),
        (LogoSource::LogoManager, _) => {
            format!("Logo Manager has an exact name match {:?}", channel.name)
        }
        (LogoSource::TvLogos, Some(hit)) => match hit.score {
            Some(score) => format!(
                "tv-logos filename match {}/{} ({score:.0})",
                hit.dir, hit.file
            ),
            None => format!("tv-logos station file {}/{}", hit.dir, hit.file),
        },
        (LogoSource::TvLogos, None) => "the tv-logos filename match".to_string(),
        (LogoSource::Network, Some(hit)) => format!(
            "the station's network logo {}/{} (no station file)",
            hit.dir, hit.file
        ),
        (LogoSource::Network, None) => "the station's network logo".to_string(),
        (LogoSource::Default, _) => format!(
            "the configured default logo {:?}",
            settings.default_logo.trim()
        ),
    };
    let existing = lookups.by_url.get(resolved.url.as_str()).map(|(id, _)| *id);
    row(
        Some(resolved.url),
        Some(resolved.source),
        existing,
        resolved.tv_hit,
        reason,
    )
}

fn channel_logo_name(channel: &ScopedChannel, lookups: &LogoWorld<'_>) -> Option<String> {
    let id = channel.logo_id?;
    if id == 0 {
        return None;
    }
    lookups.by_id.get(&id).map(|(name, _)| (*name).to_string())
}

/// Whether the channel carries a logo the operator chose (CM's `has_no_logo`,
/// `plugin.py:892`: `None`, `0` or `"0"` read as none; REST only sends the
/// first two).
fn has_logo_id(logo_id: Option<i64>) -> bool {
    !matches!(logo_id, None | Some(0))
}

/// One channel's final outcome: the assignment (or the skip), post-plan.
#[derive(Debug, Clone)]
pub struct LogoOutput {
    pub id: i64,
    pub number: Option<f64>,
    pub group: String,
    pub old: String,
    pub old_logo: String,
    pub new_logo: String,
    pub source: String,
    /// `assign`, `planned`, `failed` or `skipped`.
    pub status: String,
    pub reason: String,
    pub callsign: Option<String>,
    pub country: Option<String>,
    pub canonical: String,
}

/// Plan the assignments as one per-channel `Set Logo` through the shared planner.
///
/// Pure: nothing is written. Callers pass only the rows whose logo id is
/// already known (existing logos for a preview; existing plus just-created
/// logos, resolved against a world augmented with the creates, for an apply).
pub fn plan_assignments(
    world: &World,
    selection: &Selection,
    rows: &[&LogoRow],
    logo_ids: &HashMap<String, i64>,
) -> Result<channel_ops::Plan> {
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for row in rows {
        if let Some(url) = row.url.as_deref() {
            if let Some(id) = logo_ids.get(url) {
                values.insert(row.id, SetValue::Logo(Some(*id)));
            }
        }
    }
    if values.is_empty() {
        return Ok(channel_ops::Plan::default());
    }
    let ops = vec![Op::Set {
        field: Field::Logo,
        values,
    }];
    channel_ops::plan_with_options(world, selection, &ops, &PlanOptions::default())
}

/// Final per-channel outcomes from the logo rows plus the plans.
pub fn final_outputs(
    rows: &[LogoRow],
    assignable: &[&LogoRow],
    plan: &channel_ops::Plan,
    applied: Option<&channel_ops::ApplyReport>,
) -> Vec<LogoOutput> {
    let assignable_ids: HashSet<i64> = assignable.iter().map(|row| row.id).collect();
    let planned: HashMap<i64, &channel_ops::Change> = plan
        .changes
        .iter()
        .map(|change| (change.channel_id, change))
        .collect();
    rows.iter()
        .map(|row| {
            let (status, new_logo, source, reason) = match row.url.as_deref() {
                None => (
                    "skipped".to_string(),
                    String::new(),
                    String::new(),
                    row.reason.clone(),
                ),
                Some(url) => {
                    if !assignable_ids.contains(&row.id) {
                        (
                            "planned".to_string(),
                            url.to_string(),
                            row.source
                                .map(|s| s.label().to_string())
                                .unwrap_or_default(),
                            row.reason.clone(),
                        )
                    } else if let Some(change) = planned.get(&row.id) {
                        let status = match applied {
                            None => "planned",
                            Some(_) => match change.status {
                                channel_ops::RowStatus::Ok => "assign",
                                channel_ops::RowStatus::Failed => "failed",
                                channel_ops::RowStatus::Planned => "planned",
                            },
                        };
                        let reason = if applied.is_some() && !change.reason.is_empty() {
                            change.reason.clone()
                        } else {
                            row.reason.clone()
                        };
                        (
                            status.to_string(),
                            url.to_string(),
                            row.source
                                .map(|s| s.label().to_string())
                                .unwrap_or_default(),
                            reason,
                        )
                    } else {
                        (
                            "skipped".to_string(),
                            url.to_string(),
                            row.source
                                .map(|s| s.label().to_string())
                                .unwrap_or_default(),
                            "the channel already carries this logo".to_string(),
                        )
                    }
                }
            };
            LogoOutput {
                id: row.id,
                number: row.number,
                group: row.group.clone(),
                old: row.old.clone(),
                old_logo: row
                    .current_logo
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
                new_logo,
                source,
                status,
                reason,
                callsign: row.identity.callsign.clone(),
                country: row.identity.country.clone(),
                canonical: row.identity.canonical_name.clone(),
            }
        })
        .collect()
}

/// Load the tv-logos index: the injected fixture, or one fetched tree.
///
/// `None` (no fetch, no fixture) when the tv-logos tier is not consulted —
/// callers skip the network entirely for EPG/manager/default-only runs.
pub async fn load_tv_index(
    data_dir: &std::path::Path,
    injected: Option<TvLogosIndex>,
    needed: bool,
) -> Result<Option<TvLogosIndex>> {
    if !needed {
        return Ok(None);
    }
    if let Some(index) = injected {
        return Ok(Some(index));
    }
    let cache = FetchCache::tv_logos(data_dir)?;
    let fetched = cache.fetch(TV_LOGOS_TREE_URL).await?;
    let payload: serde_json::Value = serde_json::from_slice(&fetched.body)
        .map_err(|error| anyhow::anyhow!("the tv-logos tree is not JSON: {error:#}"))?;
    Ok(Some(TvLogosIndex::from_tree(&payload)))
}

/// Run `logos_preview` / `logos_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply
/// creates the missing logos first, then assigns every resolved URL, recorded
/// as run rows for the host undo.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    run_with(app, context, settings, dry_run, None, None).await
}

/// [`run`] with an injected tv-logos index (tests, no network) and creator.
#[allow(clippy::too_many_lines)]
pub async fn run_with(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
    tv_injected: Option<TvLogosIndex>,
    creator: Option<Arc<dyn ChannelCreator>>,
) -> Result<JobOutcome> {
    let action_id = if dry_run {
        crate::ACTION_LOGOS_PREVIEW
    } else {
        crate::ACTION_LOGOS_APPLY
    };
    let order = LogoSource::parse_order(&settings.logo_sources)?;
    let client = context.dispatcharr()?;
    let world: World = client
        .load_world_with(
            WorldKinds::only(WorldKind::Channels)
                | WorldKind::Groups
                | WorldKind::Logos
                | WorldKind::EpgEntries,
        )
        .await?;
    let config = context.config();

    let base = context.base_selection(&world)?;
    let scoped = context.scoped_channels(&world)?;
    let lookups = LogoWorld::of(&world);

    // The unified stack: the same engine + identity cache guide and streams run on.
    let run_engine = RunEngine::for_config(&config.matching, context.store().cloned());
    let ctx = ResolveContext::default();

    // Candidates first (a channel with a logo is out unless replacing), so an
    // EPG/manager-only run never touches the network for the tv-logos tree.
    let channels: Vec<ScopedChannel> = scoped
        .channels
        .iter()
        .map(|channel| ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
            logo_id: channel.logo_id,
            epg_id: channel.current_epg_id(),
        })
        .collect();
    let tv_tiers = order.contains(&LogoSource::TvLogos) || order.contains(&LogoSource::Network);
    let needs_tv = tv_tiers
        && channels
            .iter()
            .any(|channel| !has_logo_id(channel.logo_id) || settings.replace_existing);
    let tv_index = load_tv_index(&context.data_dir(), tv_injected, needs_tv).await?;
    let enabled: Vec<String> = config.matching.databases();
    let sources = lookups.sources(order.clone(), tv_index.as_ref(), settings);

    // One resolve per channel, on every core (the tv-logos fuzzy tier is the costly
    // part: one similarity pass per candidate file per channel).
    let rows: Vec<LogoRow> = channels
        .par_iter()
        .map(|channel| {
            let identity = run_engine.resolve(&channel.name, &ctx);
            let codes = tv_countries(settings, &identity, &enabled);
            decide_logo(channel, &identity, &lookups, &sources, &codes, settings)
        })
        .collect();

    // Rows that resolve to a URL: an already-carried URL is a skip (CM's
    // `already_set`), everything else assigns.
    let mut assign: Vec<&LogoRow> = Vec::new();
    let mut skipped_same: HashSet<i64> = HashSet::new();
    for row in &rows {
        let Some(url) = row.url.as_deref() else {
            continue;
        };
        let current = channels
            .iter()
            .find(|channel| channel.id == row.id)
            .and_then(|channel| channel.logo_id)
            .and_then(|id| lookups.by_id.get(&id))
            .map(|(_, url)| (*url).to_string());
        if current.as_deref() == Some(url) {
            skipped_same.insert(row.id);
        } else {
            assign.push(row);
        }
    }

    // New URLs become create-stage logos, owned by lineup, named for the first
    // channel that needs them (CM's `Logo.objects.create(name=name, url=...)`).
    let mut new_urls: Vec<String> = Vec::new();
    let mut seen_urls: HashSet<String> = HashSet::new();
    for row in &assign {
        if let Some(url) = row.url.as_deref() {
            if !lookups.by_url.contains_key(url) && seen_urls.insert(url.to_string()) {
                new_urls.push(url.to_string());
            }
        }
    }
    let mut name_of: HashMap<&str, &str> = HashMap::new();
    for row in &assign {
        if let Some(url) = row.url.as_deref() {
            name_of.entry(url).or_insert(row.old.as_str());
        }
    }

    let owned = channel_ops::load_owned(context, &world, crate::APP_ID)?;
    let request = CreateRequest {
        app: crate::APP_ID.to_string(),
        groups: Vec::new(),
        logos: new_urls
            .iter()
            .map(|url| NewLogo {
                name: name_of
                    .get(url.as_str())
                    .copied()
                    .unwrap_or(url.as_str())
                    .to_string(),
                url: url.clone(),
                key: logo_owner_key(url),
            })
            .collect(),
        channels: Vec::new(),
    };
    let create_plan =
        channel_ops::plan_creates(&world, &owned.channels, &request, &PlanOptions::default())?;

    let creator: Arc<dyn ChannelCreator> = match creator {
        Some(creator) => creator,
        None => Arc::new(RestChannelCreator::new(
            client.clone(),
            config.dispatcharr.concurrency,
        )),
    };
    let create_report = if dry_run {
        None
    } else {
        Some(channel_ops::apply_creates(creator, &create_plan, context, action_id).await)
    };

    // The Set plan runs against the world augmented with the logos this run
    // just created (same URLs, real ids).
    let mut planned_world = world.clone();
    if let Some(report) = create_report.as_ref() {
        for (logo, outcome) in create_plan.logos.iter().zip(report.logos.iter()) {
            if let Some(id) = outcome.id {
                if logo.existing_id.is_none()
                    && !planned_world.logos.iter().any(|known| known.id == id)
                {
                    planned_world.logos.push(crosstalk_core::dispatcharr::Logo {
                        id,
                        name: logo.name.clone(),
                        url: logo.url.clone(),
                        ..Default::default()
                    });
                }
            }
        }
    }
    let logo_ids: HashMap<String, i64> = planned_world
        .logos
        .iter()
        .map(|logo| (logo.url.clone(), logo.id))
        .collect();
    // A preview can only assign logos that already exist; an apply assigns
    // everything the create stage landed. A logo the create stage refused
    // leaves its channels behind.
    let assignable: Vec<&LogoRow> = if dry_run {
        assign
            .iter()
            .filter(|row| {
                row.url
                    .as_deref()
                    .is_some_and(|url| lookups.by_url.contains_key(url))
            })
            .copied()
            .collect()
    } else {
        assign
            .iter()
            .filter(|row| {
                row.url
                    .as_deref()
                    .is_some_and(|url| logo_ids.contains_key(url))
            })
            .copied()
            .collect()
    };
    let refused: HashSet<i64> = assign
        .iter()
        .filter(|row| !assignable.iter().any(|planned| planned.id == row.id))
        .map(|row| row.id)
        .collect();
    let plan = plan_assignments(&planned_world, &base, &assignable, &logo_ids)?;
    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply(client, &plan, context, action_id).await)
    };

    let mut outputs = final_outputs(&rows, &assignable, &plan, applied.as_ref());
    for output in outputs.iter_mut() {
        if skipped_same.contains(&output.id) {
            output.status = "skipped".to_string();
            output.reason = "the channel already carries this logo".to_string();
        } else if refused.contains(&output.id) && !dry_run {
            output.status = "failed".to_string();
            output.reason = format!(
                "logo {:?} could not be created, so the channel keeps its logo",
                output.new_logo,
            );
        }
    }

    let mut assigned = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    for output in &outputs {
        match output.status.as_str() {
            "assign" | "planned" => assigned += 1,
            "failed" => failed += 1,
            _ => skipped += 1,
        }
    }
    let created = create_report
        .as_ref()
        .map(|report| {
            report
                .logos
                .iter()
                .filter(|outcome| outcome.created && outcome.status == channel_ops::RowStatus::Ok)
                .count()
        })
        .unwrap_or_else(|| {
            create_plan
                .logos
                .iter()
                .filter(|logo| logo.existing_id.is_none())
                .count()
        });
    // CM counts per tier; report the same split.
    let mut by_source: HashMap<&str, usize> = HashMap::new();
    for output in outputs
        .iter()
        .filter(|o| o.status == "assign" || o.status == "planned")
    {
        *by_source.entry(output.source.as_str()).or_default() += 1;
    }

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run {
        "logos_preview"
    } else {
        "logos_applied"
    };
    let title = if dry_run {
        "Logos Preview Report"
    } else {
        "Logos Applied Report"
    };
    let cut = crosstalk_core::engine_run::cut_setting(&config.matching);
    let exports_dir = context.exports_dir();
    let mut report = Report::new(&exports_dir, app, stem, title)
        .channels_processed(channels.len())
        .result("Selection", &scoped.filter_description)
        .result("Logo sources", tiers_description(&order, settings))
        .result("Logos to create", new_urls.len())
        .result("Assignments planned", plan.changes.len())
        .result(
            "Assignments written",
            match &applied {
                Some(report) => report.ok.to_string(),
                None => "none, this was a preview".to_string(),
            },
        )
        .result("Skipped", plan.skipped.len() + skipped)
        .setting(cut.0, cut.1);
    let mut sources: Vec<(String, usize)> = by_source
        .into_iter()
        .map(|(source, count)| {
            let name = if source.is_empty() {
                "unmatched".to_string()
            } else {
                source.to_string()
            };
            (format!("From {name}"), count)
        })
        .collect();
    sources.sort();
    for (label, count) in &sources {
        report = report.result(label, count.to_string());
    }
    let report = report.write(context.config(), &schema, &values, &COLUMNS, |writer| {
        for output in &outputs {
            writer.write_record([
                output.id.to_string(),
                output.number.map(|n| format!("{n}")).unwrap_or_default(),
                csv_report::safe_cell(&output.group),
                csv_report::safe_cell(&output.old),
                csv_report::safe_cell(&output.old_logo),
                csv_report::safe_cell(&output.new_logo),
                output.source.clone(),
                output.status.clone(),
                csv_report::safe_cell(&output.reason),
                output.callsign.clone().unwrap_or_default(),
                output.country.clone().unwrap_or_default(),
                csv_report::safe_cell(&output.canonical),
            ])?;
        }
        Ok(())
    })?;

    context.record_rows(&channel_ops::create_and_edit_run_rows(
        &create_plan,
        create_report.as_ref(),
        &plan,
        applied.as_ref(),
    ))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut summary = if dry_run {
        format!(
            "Logos preview: {assigned} assignment(s) ({created} new logo(s)), {skipped} skipped"
        )
    } else {
        let (ok, failed) = applied
            .as_ref()
            .map(|report| (report.ok, report.failed))
            .unwrap_or((0, 0));
        format!(
            "Logos apply: assigned {ok} logo(s) ({failed} failed, {created} logo(s) created), {skipped} skipped"
        )
    };
    if assigned > 0 {
        summary.push_str(&format!(" — selection: {}", scoped.filter_description));
    }
    // Replace mode wrote a JSON backup in CM; the before values live in the run
    // rows here instead, and the host undo reverses them.
    if settings.replace_existing && !dry_run {
        summary.push_str(" (previous logos recorded in the run rows, undoable)");
    }
    let mut built = OutcomeBuilder::new(summary)
        .count("channels", channels.len() as i64)
        .count("assigned", assigned as i64)
        .count("logos_created", created as i64)
        .count("skipped", skipped as i64)
        .count(
            "written",
            applied.as_ref().map(|report| report.ok as i64).unwrap_or(0),
        )
        .count(
            "failed",
            applied
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(failed as i64),
        )
        .report(Some(report));
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

/// The consulted tier order in words, for the report preamble.
fn tiers_description(order: &[LogoSource], settings: &crate::settings::LineupSettings) -> String {
    let mut labels: Vec<&str> = order.iter().map(|source| source.label()).collect();
    if !order.contains(&LogoSource::Default) && !settings.default_logo.trim().is_empty() {
        labels.push("default (fallback)");
    }
    labels.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup, EpgEntry, Logo};
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::logos::TvLogosIndex;
    use crosstalk_core::store::{RunStatus, Store};
    use crosstalk_core::subapp::Registry;
    use serde_json::json;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_settings() -> crate::settings::LineupSettings {
        crate::settings::LineupSettings::default()
    }

    /// The recorded tv-logos tree shape (mirrors `scripts/data/tv_logos_tree.json`):
    /// a country dir, a station dir, and a second country.
    pub fn fixture_tree() -> serde_json::Value {
        json!({
            "truncated": false,
            "tree": [
                {"path": "countries/united-states/cnn-us.png", "type": "blob"},
                {"path": "countries/united-states/espn-us.png", "type": "blob"},
                {"path": "countries/united-states/food-network-us.png", "type": "blob"},
                {"path": "countries/united-states/us-local/cbs-42-wiat-us.png", "type": "blob"},
                {"path": "countries/united-states/us-local/nbc-5-kalb-us.png", "type": "blob"},
                {"path": "countries/united-states/notes.txt", "type": "blob"},
                {"path": "countries/united-states", "type": "tree"},
                {"path": "countries/united-kingdom/bbc-one-uk.png", "type": "blob"},
                {"path": "countries/nordic/norway/nrk1-no.png", "type": "blob"},
            ]
        })
    }

    fn fixture_index() -> TvLogosIndex {
        TvLogosIndex::from_tree(&fixture_tree())
    }

    fn logos_world() -> World {
        World {
            channels: vec![
                Channel {
                    id: 1,
                    // EPG icon tier: entry 40 carries an icon.
                    name: "CNN".to_string(),
                    channel_number: Some(json!(1.0)),
                    channel_group_id: Some(10),
                    epg_data_id: Some(40),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    // Manager tier: the manager names it exactly.
                    name: "ESPN".to_string(),
                    channel_number: Some(json!(2.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 3,
                    // tv-logos tier: fuzzy filename match.
                    name: "Food Network".to_string(),
                    channel_number: Some(json!(3.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 4,
                    // Station tier: the OTA format takes the us-local file.
                    name: "CBS - AL Birmingham (WIAT)".to_string(),
                    channel_number: Some(json!(4.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 5,
                    // Default tier: nothing else matches.
                    name: "Zzqz No Such Channel 999".to_string(),
                    channel_number: Some(json!(5.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 6,
                    // Already has a logo: skipped unless replacing.
                    name: "HBO".to_string(),
                    channel_number: Some(json!(6.0)),
                    channel_group_id: Some(10),
                    logo_id: Some(70),
                    ..Channel::default()
                },
            ],
            groups: vec![ChannelGroup {
                id: 10,
                name: "Test".to_string(),
            }],
            entries: vec![EpgEntry {
                id: 40,
                name: "CNN entry".to_string(),
                icon_url: Some("https://example.com/epg-cnn.png".to_string()),
                ..EpgEntry::default()
            }],
            logos: vec![
                Logo {
                    id: 70,
                    name: "HBO".to_string(),
                    url: "https://example.com/hbo.png".to_string(),
                    ..Logo::default()
                },
                Logo {
                    id: 71,
                    name: "ESPN".to_string(),
                    url: "https://example.com/manager-espn.png".to_string(),
                    ..Logo::default()
                },
                Logo {
                    id: 72,
                    name: "Fallback".to_string(),
                    url: "https://example.com/default.png".to_string(),
                    ..Logo::default()
                },
            ],
            ..World::default()
        }
    }

    fn decide_all(world: &World, settings: &crate::settings::LineupSettings) -> Vec<LogoRow> {
        let order = LogoSource::parse_order(&settings.logo_sources).expect("order");
        let lookups = LogoWorld::of(world);
        let index = fixture_index();
        let sources = lookups.sources(order, Some(&index), settings);
        let config = CrosstalkConfig::default();
        let run_engine = RunEngine::for_config(&config.matching, None);
        let ctx = ResolveContext::default();
        world
            .channels
            .iter()
            .map(|channel| {
                let scoped = ScopedChannel {
                    id: channel.id,
                    name: channel.name.clone(),
                    number: channel.display_number(),
                    group: world.group_name(channel.channel_group_id),
                    logo_id: channel.logo_id,
                    epg_id: channel.current_epg_id(),
                };
                let identity = run_engine.resolve(&channel.name, &ctx);
                let codes = tv_countries(settings, &identity, &config.matching.databases());
                decide_logo(&scoped, &identity, &lookups, &sources, &codes, settings)
            })
            .collect()
    }

    fn search_tv(name: &str, codes: &[&str]) -> Option<TvHit> {
        let codes: Vec<String> = codes.iter().map(|code| code.to_string()).collect();
        logos::search_tv_logos(name, &fixture_index(), &codes, LOGO_MATCH_THRESHOLD).0
    }

    #[test]
    fn each_tier_resolves_in_order() {
        let mut settings = test_settings();
        settings.default_logo = "Fallback".to_string();
        let rows = decide_all(&logos_world(), &settings);
        let by_id: HashMap<i64, &LogoRow> = rows.iter().map(|row| (row.id, row)).collect();
        // EPG icon first: channel 1 takes the entry icon even though tv-logos
        // has cnn-us.png.
        assert_eq!(
            (by_id[&1].url.as_deref(), by_id[&1].source),
            (
                Some("https://example.com/epg-cnn.png"),
                Some(LogoSource::EpgIcon)
            )
        );
        // Manager second: exact name, case-insensitive.
        assert_eq!(
            (by_id[&2].url.as_deref(), by_id[&2].source),
            (
                Some("https://example.com/manager-espn.png"),
                Some(LogoSource::LogoManager)
            )
        );
        assert_eq!(by_id[&2].existing_logo_id, Some(71));
        // tv-logos fuzzy third.
        assert_eq!(by_id[&3].source, Some(LogoSource::TvLogos));
        assert!(
            by_id[&3]
                .url
                .as_deref()
                .unwrap_or_default()
                .ends_with("food-network-us.png"),
            "{:?}",
            by_id[&3].url
        );
        // Station-format names take the station file under its own directory.
        assert_eq!(by_id[&4].source, Some(LogoSource::TvLogos));
        assert!(
            by_id[&4]
                .url
                .as_deref()
                .unwrap_or_default()
                .ends_with("us-local/cbs-42-wiat-us.png"),
            "{:?}",
            by_id[&4].url
        );
        assert_eq!(by_id[&4].tv_hit.as_ref().and_then(|hit| hit.score), None);
        // Default last.
        assert_eq!(
            (by_id[&5].url.as_deref(), by_id[&5].source),
            (
                Some("https://example.com/default.png"),
                Some(LogoSource::Default)
            )
        );
        // A channel with a logo is skipped when not replacing.
        assert_eq!(by_id[&6].url, None);
        assert!(by_id[&6].reason.contains("already has a logo"));
    }

    #[test]
    fn the_source_order_is_the_setting() {
        // `tv-logos` first: channel 1 takes cnn-us.png over its EPG icon.
        let mut settings = test_settings();
        settings.logo_sources = "tv-logos, epg, logo_manager".to_string();
        let rows = decide_all(&logos_world(), &settings);
        let cnn = rows.iter().find(|row| row.id == 1).expect("cnn");
        assert_eq!(cnn.source, Some(LogoSource::TvLogos));
        assert!(
            cnn.url
                .as_deref()
                .unwrap_or_default()
                .ends_with("cnn-us.png"),
            "{:?}",
            cnn.url
        );
        // Unknown tokens fail naming the valid ones (the settings check too).
        let error = LogoSource::parse_order("epg, beacons").expect_err("rejects");
        assert!(format!("{error:#}").contains("logo_manager"), "{error:#}");
    }

    #[test]
    fn replace_mode_considers_channels_with_logos() {
        let mut settings = test_settings();
        settings.replace_existing = true;
        // HBO's manager name is its own logo: an "already this logo" skip needs
        // the apply loop, but the decision resolves rather than skipping.
        let rows = decide_all(&logos_world(), &settings);
        let hbo = rows.iter().find(|row| row.id == 6).expect("hbo");
        assert_eq!(hbo.url.as_deref(), Some("https://example.com/hbo.png"));
        assert_eq!(hbo.source, Some(LogoSource::LogoManager));
        // Without replace the same channel is skipped outright.
        let rows = decide_all(&logos_world(), &test_settings());
        let hbo = rows.iter().find(|row| row.id == 6).expect("hbo");
        assert_eq!(hbo.url, None);
    }

    #[test]
    fn country_dirs_follow_override_then_identity_then_enabled() {
        let settings = test_settings();
        let config = CrosstalkConfig::default();
        // The identity's country wins without an override.
        let run_engine = RunEngine::for_config(&config.matching, None);
        let mut identity = run_engine.resolve("BBC One", &ResolveContext::default());
        identity.country = Some("GB".to_string());
        assert_eq!(
            tv_countries(&settings, &identity, &config.matching.databases()),
            vec!["GB".to_string()]
        );
        // No identity country: the enabled `[matching]` countries.
        identity.country = None;
        assert_eq!(
            tv_countries(&settings, &identity, &config.matching.databases()),
            config.matching.databases()
        );
        let mut overridden = test_settings();
        overridden.tv_logos_countries = "NO".to_string();
        assert_eq!(
            tv_countries(&overridden, &identity, &config.matching.databases()),
            vec!["NO".to_string()]
        );
        // Norway lives under nordic/ in the repo (the CM fix).
        let hit = search_tv("NRK1", &["NO"]);
        assert!(
            hit.map(|hit| hit.url)
                .unwrap_or_default()
                .ends_with("nordic/norway/nrk1-no.png"),
            "NO maps to nordic/norway"
        );
        // GB uses the -uk suffix (CM's lower() left -gb unstripped).
        let hit = search_tv("BBC One", &["GB"]);
        assert!(
            hit.map(|hit| hit.url)
                .unwrap_or_default()
                .ends_with("united-kingdom/bbc-one-uk.png"),
            "GB strips -uk"
        );
        // An unknown code disables the tier for that code.
        let hit = search_tv("CNN", &["ZZ"]);
        assert_eq!(hit.map(|hit| hit.url), None);
    }

    #[test]
    fn a_decorated_name_takes_the_bare_names_file() {
        // The tier matches the name without its country marker; the normalizer
        // drops quality tags, so every decoration lands on the same file.
        for name in ["CNN", "CNN HD", "US: CNN HD", "US | CNN"] {
            assert_eq!(tv_match_name(name).trim_end_matches(" HD"), "CNN", "{name}");
            let hit = search_tv(tv_match_name(name), &["US"]);
            assert_eq!(
                hit.map(|hit| hit.file).as_deref(),
                Some("cnn-us.png"),
                "{name}"
            );
        }
    }

    #[test]
    fn logo_keys_name_the_url() {
        assert_eq!(
            logo_owner_key("https://example.com/cnn.png"),
            "lineup:logo:https://example.com/cnn.png"
        );
    }

    #[test]
    fn missing_default_is_a_skip_not_a_run_error() {
        let mut settings = test_settings();
        settings.default_logo = "Nope".to_string();
        let rows = decide_all(&logos_world(), &settings);
        let missing = rows.iter().find(|row| row.id == 5).expect("ch 5");
        assert_eq!(missing.url, None);
        assert!(
            missing.reason.contains("not in the Logo Manager"),
            "{}",
            missing.reason
        );
    }

    async fn serve(world: &World) -> (MockServer, CrosstalkConfig, tempfile::TempDir) {
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
            (
                "/api/channels/logos/",
                serde_json::to_value(&world.logos).expect("logos"),
            ),
            (
                "/api/epg/epgdata/",
                serde_json::to_value(&world.entries).expect("entries"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        // One mock per created logo (ids the ownership half of the test asserts).
        for (fragment, id, name, url) in [
            (
                "epg-cnn.png",
                8001,
                "CNN",
                "https://example.com/epg-cnn.png",
            ),
            (
                "food-network-us.png",
                8002,
                "Food Network",
                "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/food-network-us.png",
            ),
            (
                "cbs-42-wiat-us.png",
                8003,
                "CBS - AL Birmingham (WIAT)",
                "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/us-local/cbs-42-wiat-us.png",
            ),
        ] {
            Mock::given(method("POST"))
                .and(path("/api/channels/logos/"))
                .and(body_string_contains(fragment))
                .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                    "id": id,
                    "name": name,
                    "url": url,
                })))
                .mount(&server)
                .await;
        }
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        (server, config, dir)
    }

    fn context_for(
        config: &CrosstalkConfig,
        store: &Store,
        app: &str,
        action: &str,
        dry_run: bool,
    ) -> (JobContext, i64) {
        let run_id = store.start_run(app, action, dry_run).expect("start run");
        let context = JobContext::new(Arc::new(config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(
                crosstalk_core::dispatcharr::DispatcharrClient::new(&config.dispatcharr)
                    .expect("client"),
            )
            .for_run(
                app,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        (context, run_id)
    }

    #[tokio::test]
    async fn preview_writes_nothing_but_the_csv() {
        let world = logos_world();
        // No POST/PATCH routes mounted: a preview that writes anything fails loudly.
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
            (
                "/api/channels/logos/",
                serde_json::to_value(&world.logos).expect("logos"),
            ),
            (
                "/api/epg/epgdata/",
                serde_json::to_value(&world.entries).expect("entries"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        let _dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(_dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, "lineup", "logos_preview", true);
        let mut settings = test_settings();
        settings.default_logo = "Fallback".to_string();
        let outcome = run_with(&app, &context, &settings, true, Some(fixture_index()), None)
            .await
            .expect("previews");
        assert!(
            outcome.summary.contains("Logos preview"),
            "{}",
            outcome.summary
        );
        let writes: Vec<_> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() != "GET")
            .collect();
        assert!(writes.is_empty(), "a preview writes nothing");
        let path = outcome.report_path.expect("a report");
        let text = std::fs::read_to_string(&path).expect("readable");
        let mut lines = text.lines().filter(|line| !line.starts_with('#'));
        let header = lines.next().expect("header");
        assert_eq!(
            header,
            "id,channel_number,group,channel_name,old_logo,new_logo,source,status,\
             reason,callsign,country,canonical"
        );
        assert_eq!(lines.count(), 6, "every in-scope channel is a row");
    }

    #[tokio::test]
    async fn apply_dedups_logos_by_url_and_assigns() {
        let world = logos_world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "logos_apply", false);
        let mut settings = test_settings();
        settings.default_logo = "Fallback".to_string();
        let outcome = run_with(
            &app,
            &context,
            &settings,
            false,
            Some(fixture_index()),
            None,
        )
        .await
        .expect("applies");
        assert!(
            outcome.summary.contains("Logos apply"),
            "{}",
            outcome.summary
        );
        let requests = server.received_requests().await.unwrap_or_default();
        // One POST per new URL: the EPG icon (ch 1), food-network (ch 3), the
        // station file (ch 4). ESPN reuses logo 71, the default reuses 72.
        let posts: Vec<_> = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/api/channels/logos/"
            })
            .collect();
        let mut urls: Vec<String> = posts
            .iter()
            .filter_map(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body)
                    .ok()
                    .and_then(|body| {
                        body.get("url")
                            .and_then(|url| url.as_str())
                            .map(str::to_string)
                    })
            })
            .collect();
        urls.sort();
        urls.dedup();
        assert_eq!(urls.len(), posts.len(), "no URL is created twice");
        assert_eq!(urls.len(), 3, "{urls:?}");
        // Ownership: the created logos are recorded under lineup:logo:<url>.
        let owned = store.owned_keys(crate::APP_ID).expect("owned");
        assert!(
            owned
                .iter()
                .any(|(kind, _, key)| kind == "logo" && key.starts_with("lineup:logo:")),
            "{owned:?}"
        );
        // Undo reverses the assignments (the created logos are left and listed).
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let mut assigned_world = world.clone();
        for channel in assigned_world.channels.iter_mut() {
            if channel.id == 2 {
                channel.logo_id = Some(71);
            }
        }
        let registry = Registry::new(vec![Arc::new(crate::LineupApp::new())]);
        let undo = crosstalk_core::undo::plan_undo(&store, &assigned_world, run_id, &registry)
            .expect("undo plans");
        assert!(
            undo.changes.iter().any(|change| change.channel_id == 2
                && change.reverse == channel_ops::SetValue::Logo(None)),
            "{undo:?}"
        );
        assert!(
            undo.left.iter().any(|left| left.kind == "logo"),
            "created logos are left and listed: {undo:?}"
        );
    }

    #[tokio::test]
    async fn apply_without_a_tv_index_fetches_nothing_when_the_tier_is_off() {
        // EPG/manager-only runs never touch the network for the tree: no index
        // injected and no fetch possible (no routes at all), yet the run passes.
        let world = logos_world();
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
            (
                "/api/channels/logos/",
                serde_json::to_value(&world.logos).expect("logos"),
            ),
            (
                "/api/epg/epgdata/",
                serde_json::to_value(&world.entries).expect("entries"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/api/channels/logos/"))
            .and(body_string_contains("epg-cnn.png"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "id": 8001,
                "name": "CNN",
                "url": "https://example.com/epg-cnn.png",
            })))
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        let _dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(_dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, "lineup", "logos_apply", false);
        let mut settings = test_settings();
        settings.logo_sources = "epg, logo_manager".to_string();
        // No index injected: with the tv tier off, no fetch is attempted (the
        // mock would 404 any tree request, and `needed` is false anyway).
        let outcome = run_with(&app, &context, &settings, false, None, None)
            .await
            .expect("applies without a tree");
        assert!(
            outcome.summary.contains("Logos apply"),
            "{}",
            outcome.summary
        );
    }
}
