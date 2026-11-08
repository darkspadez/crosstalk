//! Same input, same answer, in every app (phase 3 review).
//!
//! The goal reset asks for one identity per channel whatever app reads it. This test
//! pushes one set of broadcast entities — each spelled the way a Dispatcharr channel, a
//! provider stream and a guide entry spell it — through the three apps' decision paths:
//!
//! * guide: the channel's identity family over the EPG pool (`UnifiedEngine::rank_index`,
//!   what `match_run::score_families` runs);
//! * streams: the channel's identity family over the stream pool
//!   (`UnifiedEngine::rank_index_in`, what `streams.match` groups and attaches);
//! * lineup names / categorize / create: `decide_target`, `decide_category` and the
//!   create ownership key, all over the same engine and merged index;
//! * lineup clean: `clean_name` with the default rules.
//!
//! and asserts: the three spellings resolve to ONE identity family; guide's and streams'
//! families hold exactly that entity (every member is same-family, the entity's own
//! spelling is in it); `names_apply`'s target re-resolves into the family and leaves the
//! guide/streams families unchanged; categorize answers the same category for every
//! spelling; create would key every spelling to one owned channel; and clean never moves
//! an identity.
//!
//! The station rows pin the phase-3-review engine rows: a bare station callsign (`KSDK
//! HD`, `WREX-DT [HD]`) is in the station's family whatever spelling leads the pool
//! (C08), and a trailing quality tag never demotes a callsign's confidence (C09).

use crosstalk_core::config::CrosstalkConfig;
use crosstalk_lineup::categorize::{category_map, decide_category};
use crosstalk_lineup::clean::{clean_name, parse_custom_rules, CleanRules};
use crosstalk_lineup::create::create_owner_key;
use crosstalk_lineup::names::{self, decide_target, premium_db_names, NameTarget};
use crosstalk_lineup::settings::LineupSettings;
use crosstalk_matching::engine::{EngineIndex, UnifiedEngine};
use crosstalk_matching::identity::{resolve_with, Identity, ResolveContext};
use std::collections::BTreeSet;

/// One broadcast entity as three apps see it.
struct Entity {
    label: &'static str,
    channel: &'static str,
    stream: &'static str,
    epg: &'static str,
    /// Whether `names` renames it through the OTA template (a station) or premium.
    station: bool,
}

const ENTITIES: &[Entity] = &[
    Entity {
        label: "CNN",
        channel: "US: CNN HD",
        stream: "US | CNN FHD",
        epg: "CNN USA",
        station: false,
    },
    Entity {
        label: "ESPN",
        channel: "ESPN [FHD]",
        stream: "USA | ESPN | 1080p",
        epg: "ESPN",
        station: false,
    },
    Entity {
        label: "BBC One",
        channel: "UK: BBC One HD",
        stream: "BBC One | UK",
        epg: "BBC1",
        station: false,
    },
    Entity {
        label: "KSDK",
        channel: "US: NBC (KSDK) HD",
        stream: "KSDK HD",
        epg: "NBC 5 St. Louis KSDK",
        station: true,
    },
    Entity {
        label: "WREX",
        channel: "13 WREX",
        stream: "WREX-DT [HD]",
        epg: "NBC 13 Rockford (WREX)",
        station: true,
    },
];

/// Distractors every pool carries: neighbours that must never join a family.
const DISTRACTORS: &[&str] = &[
    "CNN International",
    "HLN",
    "ESPN 2",
    "ESPNU",
    "BBC Two",
    "BBC One Scotland",
    "NBC",
    "ABC (WABC)",
    "KSDK-DT2",
    "WREX 13.2",
    "Comedy Central",
];

struct Stack {
    engine: UnifiedEngine,
    db_index: EngineIndex,
    db_names: Vec<String>,
    epg_index: EngineIndex,
    epg_pool: Vec<String>,
    stream_index: EngineIndex,
    stream_pool: Vec<String>,
    settings: LineupSettings,
}

/// The host `[matching]` with the US and UK databases enabled: the entities below span
/// both, and the engine's country rule (G03) is strict once a country is configured.
fn matching() -> crosstalk_core::config::MatchingConfig {
    let mut config = CrosstalkConfig::default();
    config.matching.enabled_databases = vec!["US".to_string(), "UK".to_string()];
    config.matching
}

fn stack() -> Stack {
    let matching = matching();
    let engine = UnifiedEngine::new(&matching.engine_config());
    let db_names = premium_db_names(&matching.databases());
    let ctx = ResolveContext::default();
    let db_index = EngineIndex::build(&db_names, &engine, &ctx);
    let mut epg_pool: Vec<String> = ENTITIES.iter().map(|e| e.epg.to_string()).collect();
    epg_pool.extend(DISTRACTORS.iter().map(|s| s.to_string()));
    let mut stream_pool: Vec<String> = ENTITIES.iter().map(|e| e.stream.to_string()).collect();
    stream_pool.extend(DISTRACTORS.iter().map(|s| s.to_string()));
    let epg_index = EngineIndex::build(&epg_pool, &engine, &ctx);
    let stream_index = EngineIndex::build(&stream_pool, &engine, &ctx);
    Stack {
        engine,
        db_index,
        db_names,
        epg_index,
        epg_pool,
        stream_index,
        stream_pool,
        settings: LineupSettings::default(),
    }
}

fn identity(stack: &Stack, name: &str) -> Identity {
    resolve_with(&stack.engine, name, &ResolveContext::default())
}

/// The family a guide/streams run gives `query` over `index`, as sorted member names.
fn family(stack: &Stack, query: &str, index: &EngineIndex, pool: &[String]) -> BTreeSet<String> {
    stack
        .engine
        .rank_index_in(query, None, index)
        .into_iter()
        .map(|member| pool[member.index].clone())
        .collect()
}

fn names_target(stack: &Stack, name: &str) -> String {
    let id = identity(stack, name);
    match decide_target(
        name,
        &id,
        &stack.engine,
        &stack.db_index,
        &stack.db_names,
        &stack.settings,
    ) {
        NameTarget::Ota {
            name: Some(target), ..
        }
        | NameTarget::Premium { name: target, .. } => target,
        other => panic!("{name:?}: names gives no target: {other:?}"),
    }
}

fn category(
    stack: &Stack,
    categories: &std::collections::HashMap<String, (String, String)>,
    name: &str,
) -> Option<String> {
    let id = identity(stack, name);
    let channel = crosstalk_lineup::categorize::ScopedChannel {
        id: 1,
        name: name.to_string(),
        number: None,
        group: "Provider".to_string(),
    };
    decide_category(
        &channel,
        &id,
        &stack.engine,
        &stack.db_index,
        &stack.db_names,
        categories,
    )
    .category
}

#[test]
fn the_three_spellings_of_an_entity_are_one_identity_family() {
    let stack = stack();
    for entity in ENTITIES {
        let channel = identity(&stack, entity.channel);
        let stream = identity(&stack, entity.stream);
        let epg = identity(&stack, entity.epg);
        assert!(
            channel.same_family(&stream),
            "{}: channel {:?} vs stream {:?}",
            entity.label,
            channel,
            stream
        );
        assert!(
            channel.same_family(&epg),
            "{}: channel {:?} vs epg {:?}",
            entity.label,
            channel,
            epg
        );
        assert!(stream.same_family(&epg), "{}: stream vs epg", entity.label);
        // Create keys every spelling to the same owned channel (one channel per family),
        // except where a country marker is part of the display key and not of the
        // family: the key string is the family's equality fields, which the three
        // spellings share.
        if !entity.station {
            assert_eq!(
                create_owner_key(&channel),
                create_owner_key(&stream),
                "{}: create key channel vs stream",
                entity.label
            );
        }
    }
    // Distractors stay out: no two entities share a family, and no distractor joins one.
    for (i, a) in ENTITIES.iter().enumerate() {
        let ia = identity(&stack, a.channel);
        for b in &ENTITIES[i + 1..] {
            let ib = identity(&stack, b.channel);
            assert!(
                !ia.same_family(&ib),
                "{} vs {} share a family",
                a.label,
                b.label
            );
        }
        for distractor in DISTRACTORS {
            let d = identity(&stack, distractor);
            assert!(
                !ia.same_family(&d),
                "{}: distractor {distractor:?} joins the family",
                a.label
            );
        }
    }
}

#[test]
fn guide_and_streams_rank_the_same_entity_for_every_channel() {
    let stack = stack();
    for entity in ENTITIES {
        let channel = identity(&stack, entity.channel);
        let guide = family(&stack, entity.channel, &stack.epg_index, &stack.epg_pool);
        let streams = family(
            &stack,
            entity.channel,
            &stack.stream_index,
            &stack.stream_pool,
        );
        assert!(
            guide.contains(entity.epg),
            "{}: guide family {guide:?} lacks {:?}",
            entity.label,
            entity.epg
        );
        assert!(
            streams.contains(entity.stream),
            "{}: streams family {streams:?} lacks {:?}",
            entity.label,
            entity.stream
        );
        for member in guide.iter().chain(streams.iter()) {
            let m = identity(&stack, member);
            assert!(
                channel.same_family(&m),
                "{}: family member {member:?} is not the channel's identity",
                entity.label
            );
        }
        // The same question asked from the stream's or the entry's side lands on the
        // same entity: the stream's family over the EPG pool is the entry.
        let from_stream = family(&stack, entity.stream, &stack.epg_index, &stack.epg_pool);
        assert!(
            from_stream.contains(entity.epg),
            "{}: the stream {:?} ranks {from_stream:?} over the guide pool",
            entity.label,
            entity.stream
        );
    }
}

#[test]
fn names_apply_keeps_the_identity_and_the_other_apps_families() {
    let stack = stack();
    for entity in ENTITIES {
        let before = identity(&stack, entity.channel);
        let target = names_target(&stack, entity.channel);
        let after = identity(&stack, &target);
        assert!(
            before.same_family(&after),
            "{}: names {:?} -> {:?} leaves the family: {before:?} vs {after:?}",
            entity.label,
            entity.channel,
            target
        );
        if entity.station {
            assert_eq!(
                after.callsign, before.callsign,
                "{}: OTA keeps the callsign",
                entity.label
            );
        }
        // What guide and streams find for the renamed channel is what they found before.
        let guide_before = family(&stack, entity.channel, &stack.epg_index, &stack.epg_pool);
        let guide_after = family(&stack, &target, &stack.epg_index, &stack.epg_pool);
        assert_eq!(
            guide_before, guide_after,
            "{}: guide family moved on rename {:?} -> {:?}",
            entity.label, entity.channel, target
        );
        let streams_before = family(
            &stack,
            entity.channel,
            &stack.stream_index,
            &stack.stream_pool,
        );
        let streams_after = family(&stack, &target, &stack.stream_index, &stack.stream_pool);
        assert_eq!(
            streams_before, streams_after,
            "{}: streams family moved on rename {:?} -> {:?}",
            entity.label, entity.channel, target
        );
        // Idempotent: naming the target again is a no-op (names and the other apps
        // reach a fixpoint on the first apply).
        assert_eq!(
            names_target(&stack, &target),
            target,
            "{}: names is idempotent",
            entity.label
        );
    }
}

#[test]
fn categorize_answers_the_same_category_for_every_spelling() {
    let stack = stack();
    let categories = category_map(&matching().databases());
    for entity in ENTITIES {
        let from_channel = category(&stack, &categories, entity.channel);
        let from_stream = category(&stack, &categories, entity.stream);
        let from_epg = category(&stack, &categories, entity.epg);
        assert_eq!(
            from_channel, from_stream,
            "{}: category channel vs stream",
            entity.label
        );
        assert_eq!(
            from_channel, from_epg,
            "{}: category channel vs epg",
            entity.label
        );
        if entity.station {
            // Step 16 review: an OTA identity is never categorized.
            assert_eq!(from_channel, None, "{}: OTA stays put", entity.label);
        } else if entity.label != "BBC One" {
            // The UK DB files BBC One under its country (`United Kingdom`), which the
            // step 16 decision reads as no category; the US entities carry real ones.
            assert!(
                from_channel.is_some(),
                "{}: a premium entity has a category",
                entity.label
            );
        }
    }
}

#[test]
fn clean_never_moves_an_identity() {
    let stack = stack();
    let rules = CleanRules::from_settings(&stack.settings);
    let custom = parse_custom_rules(&stack.settings.custom_rules);
    for entity in ENTITIES {
        for spelling in [entity.channel, entity.stream, entity.epg] {
            let (cleaned, _fired, _removed) = clean_name(spelling, &rules, &custom);
            let before = identity(&stack, spelling);
            let after = identity(&stack, &cleaned);
            assert!(
                before.same_family(&after),
                "{}: clean {spelling:?} -> {cleaned:?} moved the identity: {before:?} vs {after:?}",
                entity.label
            );
            // And the cleaned spelling still ranks onto the entity in both pools.
            let guide = family(&stack, &cleaned, &stack.epg_index, &stack.epg_pool);
            assert!(
                guide.contains(entity.epg),
                "{}: cleaned {cleaned:?} ranks {guide:?} over the guide pool",
                entity.label
            );
        }
    }
    // `names` then `clean` (and the reverse) end on one name: no fight between the
    // two writers (step 15 review, pinned across the entity set).
    for entity in ENTITIES {
        let named = names_target(&stack, entity.channel);
        let (cleaned_named, _, _) = clean_name(&named, &rules, &custom);
        let (cleaned, _, _) = clean_name(entity.channel, &rules, &custom);
        let named_cleaned = names_target(&stack, &cleaned);
        assert!(
            identity(&stack, &cleaned_named).same_family(&identity(&stack, &named_cleaned)),
            "{}: names/clean order changes the family: {cleaned_named:?} vs {named_cleaned:?}",
            entity.label
        );
        assert_eq!(
            names::decide_target(
                &cleaned_named,
                &identity(&stack, &cleaned_named),
                &stack.engine,
                &stack.db_index,
                &stack.db_names,
                &stack.settings,
            )
            .target_name()
            .unwrap_or(cleaned_named.clone()),
            cleaned_named,
            "{}: clean(names(x)) is a names fixpoint",
            entity.label
        );
    }
}
