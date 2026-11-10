//! The programme pre-seed shared by match, heal and scan (audit E-B1).
//!
//! Every one of them asks [`DispatcharrClient::has_programs`] per EPG entry, and on a big
//! install that is tens of thousands of searches. Before a stage asks, the entries it could
//! ask about go through [`DispatcharrClient::seed_programs`]: one horizon request per source
//! and one airing-now request per 50 entries settle most of them, exactly as the search would,
//! and only the rest fall back to the per-entry search.
//!
//! Dummy sources stay on the per-entry path (their programmes are generated, not stored), and
//! so does a source whose type the listing does not say: the seed only answers for sources it
//! can tell are real guides.

use crosstalk_core::dispatcharr::{DispatcharrClient, EpgEntry, ProgramSeed, ProgramWindow, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use std::collections::HashSet;

/// Seed the client's programme memo for `entries` over `window`, publishing progress under
/// `action`. Never fails: whatever the seed cannot settle is asked per entry, as before.
pub(crate) async fn seed<'a>(
    context: &JobContext,
    client: &DispatcharrClient,
    world: &World,
    entries: impl IntoIterator<Item = &'a EpgEntry>,
    window: &ProgramWindow,
    action: &str,
) -> ProgramSeed {
    let seedable: HashSet<i64> = world
        .sources
        .iter()
        .filter(|source| !source.is_dummy() && !source.source_type.trim().is_empty())
        .map(|source| source.id)
        .collect();
    let asks: Vec<(i64, String, i64)> = entries
        .into_iter()
        .filter_map(|entry| {
            let source = entry.epg_source?;
            seedable
                .contains(&source)
                .then(|| (entry.id, entry.tvg_id.clone(), source))
        })
        .collect();
    if asks.is_empty() {
        return ProgramSeed::default();
    }
    let concurrency = context.config().dispatcharr.concurrency.max(1);
    let seed = client
        .seed_programs(
            &asks,
            window,
            concurrency,
            || context.is_cancelled(),
            |done, total| {
                context.progress(&ProgressEvent::new(
                    action,
                    done as u64,
                    total as u64,
                    "reading what is airing",
                ))
            },
        )
        .await;
    tracing::debug!(
        action,
        entries = seed.entries,
        requests = seed.requests(),
        empty_sources = seed.empty_sources,
        seeded_no = seed.seeded_no,
        seeded_yes = seed.seeded_yes,
        "programme pre-seed"
    );
    seed
}

#[cfg(test)]
mod tests {
    use crate::settings::GuideSettings;
    use crate::test_support::{self, Fixture, ESPN_HD};
    use crate::GuideApp;
    use crosstalk_core::dispatcharr::{Channel, EpgEntry, EpgSource, World};
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, Request, Respond, ResponseTemplate};

    /// `POST /epg/current-programs/`: entries `airing(id)` says yes to have a programme on
    /// now, every twentieth entry answers the `parsing` marker, the rest nothing.
    struct Airing(fn(i64) -> bool);

    impl Respond for Airing {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let now = chrono::Utc::now();
            let programs: Vec<serde_json::Value> = body["epg_data_ids"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_i64)
                .filter_map(|id| {
                    if id % 20 == 0 {
                        return Some(serde_json::json!({"epg_data_id": id, "parsing": true}));
                    }
                    (self.0)(id).then(|| {
                        serde_json::json!({
                            "epg_data_id": id,
                            "title": "Something",
                            "start_time": (now - chrono::Duration::minutes(10)).to_rfc3339(),
                            "end_time": (now + chrono::Duration::minutes(20)).to_rfc3339(),
                            "tvg_id": tvg_id(id),
                        })
                    })
                })
                .collect();
            ResponseTemplate::new(200).set_body_json(programs)
        }
    }

    fn tvg_id(id: i64) -> String {
        if id == ESPN_HD {
            return "espn.us".to_string();
        }
        format!("ch{id}.tv")
    }

    async fn scan(fixture: &Fixture, world: &World) -> crate::scan::ScanOutcome {
        let scoped = fixture.scoped(world);
        crate::scan::run(
            &GuideApp::new(),
            &fixture.context(),
            &GuideSettings::default(),
            world,
            &scoped,
        )
        .await
        .expect("scan")
    }

    async fn searches(fixture: &Fixture) -> usize {
        fixture
            .queries("GET", "/api/epg/programs/search/")
            .await
            .len()
    }

    #[tokio::test]
    async fn the_seeded_scan_finds_what_the_per_entry_scan_finds() {
        let mut found = Vec::new();
        for seeded in [false, true] {
            let mut world = test_support::world();
            if seeded {
                world.sources[0].source_type = "xmltv".to_string();
            }
            let fixture = Fixture::start(&world, &[ESPN_HD]).await;
            Mock::given(method("POST"))
                .and(path("/api/epg/current-programs/"))
                .respond_with(Airing(|id| id == ESPN_HD))
                .mount(&fixture.server)
                .await;
            let result = scan(&fixture, &world).await;
            let ids: Vec<i64> = result.rows.iter().map(|row| row.channel_id).collect();
            assert_eq!(ids, test_support::missing_ids());
            let espn_searched = fixture
                .queries("GET", "/api/epg/programs/search/")
                .await
                .iter()
                .any(|query| query.contains("espn.us"));
            // The airing-now answer settles ESPN; only the unseeded scan searches for it.
            assert_eq!(espn_searched, !seeded);
            found.push(ids);
        }
        assert_eq!(found[0], found[1]);
    }

    #[tokio::test]
    async fn a_dummy_source_stays_on_the_per_entry_search() {
        let mut world = test_support::world();
        world.sources[0].source_type = "dummy".to_string();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let result = scan(&fixture, &world).await;
        let ids: Vec<i64> = result.rows.iter().map(|row| row.channel_id).collect();
        assert_eq!(ids, test_support::missing_ids());
        assert!(fixture
            .queries("POST", "/api/epg/current-programs/")
            .await
            .is_empty());
        assert_eq!(searches(&fixture).await, 3, "one search per assigned entry");
    }

    /// 20k entries on four sources, the fourth with nothing in the window: the scan's searches
    /// are bounded by the entries the seed could not settle plus one probe per source.
    #[tokio::test]
    async fn a_twenty_thousand_entry_scan_searches_only_what_the_seed_left() {
        const ENTRIES: i64 = 20_000;
        let source_of = |id: i64| (id % 4) + 1;
        let sources: Vec<EpgSource> = (1..=4)
            .map(|id| EpgSource {
                id,
                name: format!("Source {id}"),
                is_active: true,
                source_type: "xmltv".to_string(),
                ..EpgSource::default()
            })
            .collect();
        let entries: Vec<EpgEntry> = (1..=ENTRIES)
            .map(|id| EpgEntry {
                id,
                name: format!("Channel {id}"),
                tvg_id: tvg_id(id),
                epg_source: Some(source_of(id)),
                icon_url: None,
            })
            .collect();
        let channels: Vec<Channel> = (1..=ENTRIES)
            .map(|id| Channel {
                id,
                name: format!("Channel {id}"),
                channel_number: Some(serde_json::json!(id)),
                epg_data_id: Some(id),
                ..Channel::default()
            })
            .collect();
        let world = World {
            channels,
            sources,
            entries,
            ..World::default()
        };
        let fixture = Fixture::start(&world, &[]).await;
        // Sources 1-3 carry programmes in the window, source 4 does not.
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("fields", "end_time"))
            .and(query_param("epg_source", "4"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"count": 0, "results": []})),
            )
            .with_priority(1)
            .mount(&fixture.server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("fields", "end_time"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"count": 1, "results": [{"end_time": "2099-01-01T00:00:00Z"}]}),
            ))
            .with_priority(2)
            .mount(&fixture.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/epg/current-programs/"))
            .respond_with(Airing(|id| id % 10 != 0))
            .mount(&fixture.server)
            .await;

        let result = scan(&fixture, &world).await;

        let on_live_source = |id: &i64| source_of(*id) != 4;
        let misses = (1..=ENTRIES)
            .filter(on_live_source)
            .filter(|id| id % 10 == 0)
            .count();
        let empty_source = (1..=ENTRIES).filter(|id| source_of(*id) == 4).count();
        assert_eq!(result.unchecked, 0);
        // Every entry on the empty source and every unsettled entry is missing (the search
        // catch-all answers "none"); everything airing is not.
        assert_eq!(result.rows.len(), empty_source + misses);
        let searched = searches(&fixture).await;
        assert!(
            searched <= misses + 4,
            "{searched} searches for {misses} unsettled entries on 4 sources"
        );
        let batches = fixture
            .queries("POST", "/api/epg/current-programs/")
            .await
            .len();
        let live = (1..=ENTRIES).filter(on_live_source).count();
        assert_eq!(batches, live.div_ceil(50));
    }
}
