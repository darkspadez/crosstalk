//! The one quality model: tiers every app reads, one sort key that orders.
//!
//! Phase 3 step 7 (goal reset, "Also unified"). Three plugins each graded
//! quality their own way:
//!
//! * IPTV Checker graded probed streams by WIDTH (`_get_stream_format`,
//!   `reference/Dispatcharr-IPTV-Checker-Plugin/iptv_checker/plugin.py:4644`):
//!   `>= 3800` UHD, `>= 1900` FHD, `>= 1200` HD, `> 0` SD, else Unknown — and
//!   appended that label as the `[UHD]`/`[FHD]`/`[HD]`/`[SD]`/`[Unknown]`
//!   format suffix triage then stripped and re-applied;
//! * Lineuparr graded streams by HEIGHT-or-name (`_get_quality_tier`,
//!   `reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/plugin.py:1651`):
//!   `>= 2160` tier 0, `>= 1080` tier 1, `>= 720` tier 2, `>= 480` tier 3,
//!   `> 0` tier 4 (Low), else name tags (`4K`/`UHD` → 0, `FHD`/`1080` → 1,
//!   word `HD` → 2, word `SD` → 3), else tier 5 (Unknown);
//! * Stream-Mapparr never tiered: its sort key (`crates/streams/src/quality.rs`,
//!   the port of `Plugin._sort_streams_by_quality`) orders by resolution tier
//!   (HD+30fps / HD-or-30fps / SD / no-resolution), M3U priority, audio ranks,
//!   pixels, frame rate, bitrate and throughput.
//!
//! [`QualityTier`] is the shared projection: six tiers both width rules (IC) and
//! height-or-name rules (LU) land on. The width and height constructors
//! reproduce their plugin EXACTLY (the probe parity harness pins the width
//! one through [`stream_format_of`], which is what `probe` calls); the name
//! constructor classifies through the unified tag vocabulary
//! ([`crosstalk_matching::tags`]) instead of LU's substring scan — `MUHD`
//! contains `UHD` as a substring but is not a quality mark, so tokens decide.
//! The one deliberate difference from LU is documented on [`from_name`].
//!
//! Stream-Mapparr's full sort key stays THE stream ordering: tiers are a
//! projection of it, not a replacement. The sort tier (HD+30fps …) folds frame
//! rate in, so it cannot be read off a [`QualityTier`] alone; the projection
//! direction is tier ← sort inputs, via [`from_dimensions`]. Step 9 migrates
//! streams' format suffix onto [`stream_format_of`]; `sort` keeps its key.

use crosstalk_matching::tags::{classify_token, TagKind};

/// IPTV Checker's five format labels (`STANDARD_QUALITY_TAGS`, `plugin.py:3325`),
/// in tier order: what [`QualityTier::ic_label`] returns and what triage's
/// format suffix and restore's tag strip treat as standard.
pub const IC_FORMAT_LABELS: [&str; 5] = ["UHD", "FHD", "HD", "SD", "Unknown"];

/// One quality tier, best first.
///
/// `Low` is Lineuparr's tier 4: a picture too small for SD (`height > 0` but
/// below 480). IPTV Checker's width rule never produces it (any positive width
/// is at least SD); [`ic_label`](QualityTier::ic_label) projects it onto `SD`,
/// the band IC's width rule gives a sub-480 picture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QualityTier {
    Uhd,
    Fhd,
    Hd,
    Sd,
    Low,
    Unknown,
}

impl QualityTier {
    /// IPTV Checker's `_get_stream_format`, verbatim: width bands only.
    ///
    /// `>= 3800` UHD, `>= 1900` FHD, `>= 1200` HD, `> 0` SD, else Unknown. A
    /// negative width is not a picture, so it reads Unknown like zero does.
    pub fn from_width(width: i64) -> Self {
        if width >= 3800 {
            QualityTier::Uhd
        } else if width >= 1900 {
            QualityTier::Fhd
        } else if width >= 1200 {
            QualityTier::Hd
        } else if width > 0 {
            QualityTier::Sd
        } else {
            QualityTier::Unknown
        }
    }

    /// Lineuparr's `_get_quality_tier` probed branch, verbatim: height bands.
    ///
    /// `>= 2160` UHD, `>= 1080` FHD, `>= 720` HD, `>= 480` SD, `> 0` Low, else
    /// Unknown.
    pub fn from_height(height: i64) -> Self {
        if height >= 2160 {
            QualityTier::Uhd
        } else if height >= 1080 {
            QualityTier::Fhd
        } else if height >= 720 {
            QualityTier::Hd
        } else if height >= 480 {
            QualityTier::Sd
        } else if height > 0 {
            QualityTier::Low
        } else {
            QualityTier::Unknown
        }
    }

    /// Lineuparr's `_get_quality_tier` name fallback, via the tag vocabulary.
    ///
    /// Every whitespace/comma/pipe-separated token is classified; the BEST tier
    /// any quality token maps to wins (`4K`/`8K`/`UHD` → UHD, `FHD`/`1080p`/
    /// `1080` → FHD, `HD` → HD, `SD` → SD). Bracketed tokens count (`[HD]`
    /// classifies); bare `1080` counts (it is in the vocabulary with source LU).
    ///
    /// Deliberate difference from LU: LU scans SUBSTRINGS (`'4K' in name`), so
    /// `MUHD-4` reads UHD there and Unknown here. Tokens are the unified
    /// reading (a substring is not a mark), and step 16's oracle report carries
    /// the diff.
    pub fn from_name(name: &str) -> Self {
        let mut best = QualityTier::Unknown;
        for token in name.split(|c: char| !(c.is_alphanumeric() || c == '+' || c == '.')) {
            if token.is_empty() {
                continue;
            }
            let Some(tag) = classify_token(token) else {
                continue;
            };
            if tag.kind != TagKind::Quality {
                continue;
            }
            // Name Cleaner's superscript marks read as the tier they spell
            // (`ᵁᴴᴰ`/`⁴ᴷ` UHD, `ᴴᴰ` HD, `ᔕᗪ` SD).
            let tier = match tag.canonical.as_str() {
                "UHD" | "4K" | "8K" | "\u{1d41}\u{1d34}\u{1d30}" | "\u{2074}\u{1d37}" => {
                    QualityTier::Uhd
                }
                "FHD" | "FHD1080" | "1080P" | "1080" => QualityTier::Fhd,
                "HD" | "\u{1d34}\u{1d30}" => QualityTier::Hd,
                "SD" | "\u{1515}\u{15ea}" => QualityTier::Sd,
                _ => continue,
            };
            if tier < best {
                best = tier;
            }
        }
        best
    }

    /// The tier for probed dimensions: height when there is one (LU's branch),
    /// else width (IC's branch), else Unknown.
    ///
    /// A stream that states both usually agrees on both (`1920x1080` is FHD
    /// either way); where they disagree the height wins, because a height IS a
    /// direct tier read while a width is a band inference. IC's own call sites
    /// keep calling [`from_width`](QualityTier::from_width) (its rule reads the
    /// resolution STRING's width only), so this constructor is for NEW shared
    /// uses, not a silent change to probe output.
    pub fn from_dimensions(width: i64, height: i64) -> Self {
        if height > 0 {
            QualityTier::from_height(height)
        } else if width > 0 {
            QualityTier::from_width(width)
        } else {
            QualityTier::Unknown
        }
    }

    /// IPTV Checker's five format labels: what triage appends and strips.
    ///
    /// `Low` (height-only, which IC's width rule cannot see) projects onto
    /// `SD`: IC's width bands give any sub-480 picture SD.
    pub fn ic_label(self) -> &'static str {
        match self {
            QualityTier::Uhd => "UHD",
            QualityTier::Fhd => "FHD",
            QualityTier::Hd => "HD",
            QualityTier::Sd | QualityTier::Low => "SD",
            QualityTier::Unknown => "Unknown",
        }
    }

    /// Lineuparr's tier number (`_get_quality_tier` 0–5): 0 UHD … 4 Low, 5 Unknown.
    pub fn lu_tier(self) -> u8 {
        match self {
            QualityTier::Uhd => 0,
            QualityTier::Fhd => 1,
            QualityTier::Hd => 2,
            QualityTier::Sd => 3,
            QualityTier::Low => 4,
            QualityTier::Unknown => 5,
        }
    }

    /// Parse an IC/LU tier label back (`UHD` → UHD, …, `Unknown` → Unknown).
    ///
    /// `None` for anything else. `Low` has no label (it never appears in a
    /// name suffix); it round-trips through [`lu_tier`](QualityTier::lu_tier).
    pub fn parse_label(label: &str) -> Option<Self> {
        match label.trim().to_uppercase().as_str() {
            "UHD" | "4K" | "8K" => Some(QualityTier::Uhd),
            "FHD" | "1080" | "1080P" => Some(QualityTier::Fhd),
            "HD" | "720P" => Some(QualityTier::Hd),
            "SD" | "480P" => Some(QualityTier::Sd),
            "UNKNOWN" | "UNK" => Some(QualityTier::Unknown),
            _ => None,
        }
    }
}

/// IPTV Checker's format label for a resolution string (`WxH`).
///
/// `_get_stream_format` reads the width before the `x` and bands it
/// ([`QualityTier::from_width`]); anything unparseable is Unknown. This is what
/// `probe` records per stream and what triage's suffix appends — one function,
/// so the two can never disagree about what `1920x1080` means.
///
/// The parse is deliberately untrimmed (`"1920 "` is Unknown): the streams port
/// this extracts from used `str::parse` directly, and identical output is the
/// contract (the probe parity harness pins it).
pub fn stream_format_of(resolution: &str) -> &'static str {
    let width = resolution
        .split_once('x')
        .and_then(|(width, _)| width.parse::<i64>().ok())
        .unwrap_or(0);
    QualityTier::from_width(width).ic_label()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn width_bands_are_iptv_checkers() {
        // plugin.py:4644 `_get_stream_format`: width-only bands.
        for (width, label) in [
            (3840, "UHD"),
            (8000, "UHD"),
            (3800, "UHD"),
            (3799, "FHD"),
            (1920, "FHD"),
            (1900, "FHD"),
            (1899, "HD"),
            (1280, "HD"),
            (1200, "HD"),
            (1199, "SD"),
            (720, "SD"),
            (1, "SD"),
            (0, "Unknown"),
            (-100, "Unknown"),
        ] {
            assert_eq!(QualityTier::from_width(width).ic_label(), label, "{width}");
            assert_eq!(
                QualityTier::from_width(width),
                QualityTier::from_width(width),
                "deterministic"
            );
        }
    }

    #[test]
    fn height_bands_are_lineuparrs() {
        // plugin.py:1651 probed branch.
        for (height, tier) in [
            (2160, QualityTier::Uhd),
            (1080, QualityTier::Fhd),
            (1079, QualityTier::Hd),
            (720, QualityTier::Hd),
            (719, QualityTier::Sd),
            (480, QualityTier::Sd),
            (479, QualityTier::Low),
            (1, QualityTier::Low),
            (0, QualityTier::Unknown),
        ] {
            assert_eq!(QualityTier::from_height(height), tier, "{height}");
            assert_eq!(tier.lu_tier(), QualityTier::from_height(height).lu_tier());
        }
        assert_eq!(
            [0, 1, 2, 3, 4, 5],
            [
                QualityTier::Uhd.lu_tier(),
                QualityTier::Fhd.lu_tier(),
                QualityTier::Hd.lu_tier(),
                QualityTier::Sd.lu_tier(),
                QualityTier::Low.lu_tier(),
                QualityTier::Unknown.lu_tier(),
            ]
        );
    }

    #[test]
    fn names_classify_through_the_vocabulary() {
        assert_eq!(QualityTier::from_name("ESPN 4K"), QualityTier::Uhd);
        assert_eq!(QualityTier::from_name("ESPN [UHD]"), QualityTier::Uhd);
        assert_eq!(QualityTier::from_name("ESPN FHD"), QualityTier::Fhd);
        assert_eq!(QualityTier::from_name("ESPN [1080p]"), QualityTier::Fhd);
        assert_eq!(QualityTier::from_name("ESPN HD"), QualityTier::Hd);
        assert_eq!(QualityTier::from_name("ESPN [SD]"), QualityTier::Sd);
        assert_eq!(QualityTier::from_name("ESPN"), QualityTier::Unknown);
        // Best tier wins; junk and status tokens are not quality.
        assert_eq!(
            QualityTier::from_name("ESPN HD [VIP] [DEAD]"),
            QualityTier::Hd
        );
        assert_eq!(QualityTier::from_name("ESPN SD HD"), QualityTier::Hd);
        // Substrings are not marks (differs from LU's `in` scan, documented).
        assert_eq!(QualityTier::from_name("MUHD-4"), QualityTier::Unknown);
        // Superscript marks spell their tier.
        assert_eq!(QualityTier::from_name("ESPN ᵁᴴᴰ"), QualityTier::Uhd);
        assert_eq!(QualityTier::from_name("ESPN ᴴᴰ"), QualityTier::Hd);
        assert_eq!(QualityTier::from_name("ESPN ⁴ᴷ"), QualityTier::Uhd);
    }

    #[test]
    fn dimensions_prefer_height() {
        assert_eq!(QualityTier::from_dimensions(1920, 1080), QualityTier::Fhd);
        assert_eq!(QualityTier::from_dimensions(1920, 0), QualityTier::Fhd);
        assert_eq!(QualityTier::from_dimensions(0, 0), QualityTier::Unknown);
        // Height wins on disagreement (documented).
        assert_eq!(QualityTier::from_dimensions(1900, 700), QualityTier::Sd);
    }

    #[test]
    fn labels_round_trip() {
        for tier in [
            QualityTier::Uhd,
            QualityTier::Fhd,
            QualityTier::Hd,
            QualityTier::Sd,
            QualityTier::Unknown,
        ] {
            assert_eq!(QualityTier::parse_label(tier.ic_label()), Some(tier));
        }
        assert_eq!(QualityTier::Low.ic_label(), "SD");
        let labels: Vec<&str> = [
            QualityTier::Uhd,
            QualityTier::Fhd,
            QualityTier::Hd,
            QualityTier::Sd,
            QualityTier::Unknown,
        ]
        .iter()
        .map(|tier| tier.ic_label())
        .collect();
        assert_eq!(labels, IC_FORMAT_LABELS);
        assert_eq!(QualityTier::parse_label("nope"), None);
    }

    #[test]
    fn resolution_strings_label_by_width() {
        assert_eq!(stream_format_of("3840x2160"), "UHD");
        assert_eq!(stream_format_of("1920x1080"), "FHD");
        assert_eq!(stream_format_of("1280x720"), "HD");
        assert_eq!(stream_format_of("720x480"), "SD");
        assert_eq!(stream_format_of("unknown"), "Unknown");
        assert_eq!(stream_format_of(""), "Unknown");
        assert_eq!(stream_format_of("1920"), "Unknown");
    }
}
