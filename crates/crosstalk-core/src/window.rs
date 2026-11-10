//! Schedule windows: how long one firing of a schedule may run.
//!
//! IPTV Checker's schedule had two end modes and a resume file; crosstalk keeps the semantics
//! and moves the bookkeeping into the store:
//!
//!   * **duration** — `window = "4h"` runs for four hours from the firing;
//!   * **time of day** — `until = "06:00"` runs until that wall-clock time, on the next day
//!     when it has already passed today (the plugin's `end = end + timedelta(days=1)`, which is
//!     what makes a 23:00 window ending at 06:00 eight hours long rather than minus seventeen).
//!
//! The cron expression is still UTC, because that is what the scheduler underneath fires on and
//! pretending otherwise would make the manifest and the firing disagree. A window's `until` is
//! read in its `timezone` key, which defaults to UTC: operators who think in local time can say
//! `timezone = "America/New_York"` on that one entry without moving every schedule.
//!
//! Dispatcharr's own timezone (step 7's [`crate::dispatcharr::DispatcharrClient::system_timezone`])
//! is deliberately NOT used here: it describes the *guide's* day, which `streams.match`'s EPG
//! date gate already consults, and reading it would make a host schedule depend on a reachable
//! server. A window is a host fact.

use anyhow::{bail, Result};
use chrono::{DateTime, Days, LocalResult, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use std::time::Duration;

/// When a window ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// A fixed run length from the firing: `window = "4h"`.
    After(Duration),
    /// A wall-clock time, on the next day when it has already passed: `until = "06:00"`.
    At(NaiveTime),
}

/// A schedule entry's window, parsed and ready to arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub end: End,
    /// The zone `until` is read in. Always set (UTC when the entry says nothing), so a report
    /// can print it without a second lookup.
    pub timezone: Tz,
}

impl Window {
    /// Parse one schedule entry's window fields.
    ///
    /// `Ok(None)` means the entry carries no window at all, which is an ordinary cron schedule:
    /// a run without a deadline resumes from a checkpoint but never pauses by itself.
    pub fn parse(
        window: Option<&str>,
        until: Option<&str>,
        timezone: Option<&str>,
    ) -> Result<Option<Self>> {
        let end = match (window, until) {
            (None, None) => return Ok(None),
            (Some(_), Some(_)) => bail!(
                "a window takes `window` (a duration such as \"4h\") or `until` (a time of day \
                 such as \"06:00\"), not both"
            ),
            (Some(text), None) => End::After(parse_duration(text)?),
            (None, Some(text)) => End::At(parse_time_of_day(text)?),
        };
        Ok(Some(Self {
            end,
            timezone: parse_timezone(timezone)?,
        }))
    }

    /// The instant a run that started at `start` must stop.
    ///
    /// A duration is exact. A time of day already passed at `start` means tomorrow, which is the
    /// past-midnight wrap `IPTV Checker._compute_window_end` does.
    pub fn deadline_from(&self, start: DateTime<Utc>) -> DateTime<Utc> {
        match self.end {
            End::After(duration) => {
                // A duration longer than chrono can express is caught when the settings are
                // parsed; clamping here keeps this total.
                let signed = chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX);
                start
                    .checked_add_signed(signed)
                    .unwrap_or(DateTime::<Utc>::MAX_UTC)
            }
            End::At(time) => {
                let local = start.with_timezone(&self.timezone);
                let today = local.date_naive();
                let candidate = self.local_instant(today, time);
                if candidate <= start {
                    self.local_instant(today + Days::new(1), time)
                } else {
                    candidate
                }
            }
        }
    }

    /// How the window reads in a report, a dashboard row or a message.
    pub fn describe(&self) -> String {
        match self.end {
            End::After(duration) => format!("{} from start", describe_duration(duration)),
            End::At(time) => format!("until {} ({})", time.format("%H:%M"), self.timezone.name()),
        }
    }

    /// A local date and time as an instant in this window's zone.
    ///
    /// The two awkward cases are a DST spring forward (the time does not exist) and a fall back
    /// (it exists twice). A skipped time ends at the first instant that does exist after it,
    /// which is what a person means by "06:00" on a night the clocks moved; a repeated time
    /// takes the earlier of the two, so a window never runs longer than it says.
    fn local_instant(&self, date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
        let naive = date.and_time(time);
        match self.timezone.from_local_datetime(&naive) {
            LocalResult::Single(instant) => instant.with_timezone(&Utc),
            LocalResult::Ambiguous(earliest, _) => earliest.with_timezone(&Utc),
            LocalResult::None => {
                for hours in 1..=3 {
                    let shifted = naive + chrono::Duration::hours(hours);
                    match self.timezone.from_local_datetime(&shifted) {
                        LocalResult::Single(instant) => return instant.with_timezone(&Utc),
                        LocalResult::Ambiguous(earliest, _) => return earliest.with_timezone(&Utc),
                        LocalResult::None => continue,
                    }
                }
                // No such instant within three hours of the requested one: read the naive value
                // as UTC rather than failing the run. Unreachable for real zones, and worse to
                // refuse a scheduled run than to end it a few hours out.
                self.timezone.from_utc_datetime(&naive).with_timezone(&Utc)
            }
        }
    }
}

/// Parse a duration as a schedule window writes it: one or more `<number><unit>` groups, in
/// `s`, `m`, `h` or `d`.
///
/// `"4h"`, `"90m"`, `"1h30m"` and `"2d"` all read; `"4"` and `"4x"` do not, because a bare
/// number has no unit and a window that silently means seconds is worse than a refusal.
pub fn parse_duration(text: &str) -> Result<Duration> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        bail!("a window duration is empty");
    }
    let mut total: f64 = 0.0;
    let mut number = String::new();
    let mut groups = 0usize;
    for character in trimmed.chars() {
        if character.is_ascii_digit() || character == '.' {
            number.push(character);
            continue;
        }
        if number.is_empty() {
            bail!("{text:?} is not a duration: expected <number><unit>, for example \"4h\"");
        }
        let value: f64 = number.parse().map_err(|_| {
            anyhow::anyhow!("{text:?} is not a duration: {number:?} is not a number")
        })?;
        let unit = match character {
            's' => 1.0,
            'm' => 60.0,
            'h' => 3_600.0,
            'd' => 86_400.0,
            other => {
                bail!("{text:?} is not a duration: {other:?} is not a unit (use s, m, h or d)")
            }
        };
        total += value * unit;
        number.clear();
        groups += 1;
    }
    if !number.is_empty() {
        bail!("{text:?} is not a duration: the last number has no unit (use s, m, h or d)");
    }
    if groups == 0 || total <= 0.0 {
        bail!("a window duration must be longer than zero, got {text:?}");
    }
    Ok(Duration::from_secs(total.round() as u64))
}

/// Parse a time of day as a window writes it: `"06:00"` or `"06:00:30"`.
pub fn parse_time_of_day(text: &str) -> Result<NaiveTime> {
    let trimmed = text.trim();
    let parts: Vec<&str> = trimmed.split(':').collect();
    let field = |index: usize| -> Result<u32> {
        parts[index].parse::<u32>().map_err(|_| {
            anyhow::anyhow!("{text:?} is not a time of day: {trimmed:?} is not hour:minute")
        })
    };
    let parsed = match parts.len() {
        2 => NaiveTime::from_hms_opt(field(0)?, field(1)?, 0),
        3 => NaiveTime::from_hms_opt(field(0)?, field(1)?, field(2)?),
        _ => bail!("{text:?} is not a time of day: expected hour:minute, for example \"06:00\""),
    };
    parsed.ok_or_else(|| {
        anyhow::anyhow!("{text:?} is not a time of day: the hour, minute or second is out of range")
    })
}

/// Parse a window's `timezone`, defaulting to UTC.
pub fn parse_timezone(name: Option<&str>) -> Result<Tz> {
    match name.map(str::trim).filter(|name| !name.is_empty()) {
        None => Ok(Tz::UTC),
        Some(name) => name.parse::<Tz>().map_err(|_| {
            anyhow::anyhow!(
                "{name:?} is not an IANA time zone name (for example \"America/New_York\")"
            )
        }),
    }
}

/// A duration in the shortest exact wording: `"4h"`, `"90m"`, `"1h30m"`, `"45s"`.
pub fn describe_duration(duration: Duration) -> String {
    let mut seconds = duration.as_secs();
    if seconds == 0 {
        return "0s".to_string();
    }
    let mut parts = Vec::new();
    for (unit, size) in [("d", 86_400u64), ("h", 3_600), ("m", 60), ("s", 1)] {
        if seconds >= size {
            parts.push(format!("{}{unit}", seconds / size));
            seconds %= size;
        }
    }
    parts.join("")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        text.parse().expect("iso 8601")
    }

    #[test]
    fn durations_read_in_seconds_minutes_hours_and_days() {
        assert_eq!(
            parse_duration("4h").expect("ok"),
            Duration::from_secs(4 * 3_600)
        );
        assert_eq!(
            parse_duration("90m").expect("ok"),
            Duration::from_secs(5_400)
        );
        assert_eq!(
            parse_duration("1h30m").expect("ok"),
            Duration::from_secs(5_400)
        );
        assert_eq!(parse_duration("45s").expect("ok"), Duration::from_secs(45));
        assert_eq!(
            parse_duration("2d").expect("ok"),
            Duration::from_secs(172_800)
        );
        // Whitespace around the value is an operator's typing, not a different window.
        assert_eq!(
            parse_duration(" 4h ").expect("ok"),
            Duration::from_secs(14_400)
        );
    }

    #[test]
    fn a_duration_without_a_unit_or_of_zero_is_refused() {
        for text in ["", "4", "4x", "0h", "-4h", "4h30", "h"] {
            assert!(parse_duration(text).is_err(), "{text:?} should not parse");
        }
    }

    #[test]
    fn a_time_of_day_reads_with_or_without_seconds() {
        assert_eq!(
            parse_time_of_day("06:00").expect("ok"),
            NaiveTime::from_hms_opt(6, 0, 0).expect("time")
        );
        assert_eq!(
            parse_time_of_day("23:59:30").expect("ok"),
            NaiveTime::from_hms_opt(23, 59, 30).expect("time")
        );
        for text in ["", "6", "06:60", "24:00", "noon", "06"] {
            assert!(
                parse_time_of_day(text).is_err(),
                "{text:?} should not parse"
            );
        }
    }

    #[test]
    fn a_window_takes_a_duration_or_a_time_of_day_but_not_both() {
        assert!(Window::parse(None, None, None)
            .expect("no window is fine")
            .is_none());
        let by_duration = Window::parse(Some("4h"), None, None)
            .expect("duration")
            .expect("a window");
        assert_eq!(by_duration.end, End::After(Duration::from_secs(14_400)));
        assert_eq!(by_duration.timezone, Tz::UTC, "a window defaults to UTC");

        let by_time = Window::parse(None, Some("06:00"), Some("America/New_York"))
            .expect("time")
            .expect("a window");
        assert_eq!(
            by_time.end,
            End::At(NaiveTime::from_hms_opt(6, 0, 0).expect("time"))
        );
        assert_eq!(by_time.timezone.name(), "America/New_York");

        let both = Window::parse(Some("4h"), Some("06:00"), None).expect_err("both");
        assert!(format!("{both}").contains("not both"), "{both}");

        assert!(Window::parse(None, Some("06:00"), Some("Mars/Olympus")).is_err());
    }

    #[test]
    fn a_duration_window_ends_that_long_after_the_firing() {
        let window = Window::parse(Some("4h"), None, None)
            .expect("window")
            .expect("some");
        let start = utc("2026-11-02T01:00:00Z");
        assert_eq!(window.deadline_from(start), utc("2026-11-02T05:00:00Z"));
        assert_eq!(window.describe(), "4h from start");
    }

    #[test]
    fn an_until_window_that_has_passed_wraps_past_midnight() {
        // IPTV Checker's `_compute_window_end`: `if end <= now: end += 1 day`. A 23:00 firing
        // that ends at 06:00 runs eight hours, not minus seventeen.
        let window = Window::parse(None, Some("06:00"), None)
            .expect("window")
            .expect("some");
        assert_eq!(
            window.deadline_from(utc("2026-11-02T23:00:00Z")),
            utc("2026-11-03T06:00:00Z")
        );
        // Started at 01:00, the same entry ends the same morning.
        assert_eq!(
            window.deadline_from(utc("2026-11-02T01:00:00Z")),
            utc("2026-11-02T06:00:00Z")
        );
        // Exactly at 06:00:00 the instant is not in the future, so it wraps: a firing at the
        // window's own end time must not produce a zero-length window.
        assert_eq!(
            window.deadline_from(utc("2026-11-02T06:00:00Z")),
            utc("2026-11-03T06:00:00Z")
        );
        assert_eq!(window.describe(), "until 06:00 (UTC)");
    }

    #[test]
    fn an_until_window_is_read_in_its_own_time_zone() {
        let window = Window::parse(None, Some("06:00"), Some("America/New_York"))
            .expect("window")
            .expect("some");
        // 2026-01-05T02:00Z is 21:00 EST on the 4th; 06:00 the next morning is 11:00Z.
        assert_eq!(
            window.deadline_from(utc("2026-01-05T02:00:00Z")),
            utc("2026-01-05T11:00:00Z")
        );
    }

    #[test]
    fn a_time_the_clocks_skip_ends_at_the_first_instant_that_exists() {
        // 2026-03-08 02:30 does not exist in New York: the clocks go 02:00 EST -> 03:00 EDT,
        // which is 07:00Z. "Until 02:30" on that night means 03:30 EDT, not a dead window.
        let window = Window::parse(None, Some("02:30"), Some("America/New_York"))
            .expect("window")
            .expect("some");
        assert_eq!(
            window.deadline_from(utc("2026-03-08T05:00:00Z")),
            utc("2026-03-08T07:30:00Z")
        );
    }

    #[test]
    fn durations_describe_themselves_in_the_shorted_exact_form() {
        assert_eq!(describe_duration(Duration::from_secs(14_400)), "4h");
        assert_eq!(describe_duration(Duration::from_secs(5_400)), "1h30m");
        assert_eq!(describe_duration(Duration::from_secs(45)), "45s");
        assert_eq!(describe_duration(Duration::from_secs(90_061)), "1d1h1m1s");
        assert_eq!(describe_duration(Duration::ZERO), "0s");
    }
}
