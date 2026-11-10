//! The channel alias table.
//!
//! Port of `aliases.py` (the built-in table) plus `Plugin._build_alias_map`'s merge rule:
//! a user-supplied entry REPLACES the built-in list for that key rather than adding to
//! it, so a custom entry has to repeat any built-in variants it still wants.
//!
//! Keys are canonical channel names; values are the alternate names providers use.

use std::collections::HashMap;

/// Built-in aliases plus any user overrides.
#[derive(Debug, Clone, Default)]
pub struct AliasMap {
    entries: HashMap<String, Vec<String>>,
}

impl AliasMap {
    /// The shipped table from `data/aliases.json` (EPG Janitor's).
    pub fn builtin() -> Self {
        Self::from_json(crate::data::ALIASES_JSON).unwrap_or_default()
    }

    /// Stream-Mapparr's shipped table, plus the country-scoped overrides its
    /// `Plugin._build_alias_map` merges for the selected channel database.
    ///
    /// The two plugins ship different alias tables (Stream-Mapparr's has 198 entries,
    /// EPG Janitor's 204, and they disagree on the variants of more than a dozen shared
    /// keys), so the profile selects the table instead of sharing one. A country override
    /// EXTENDS a key's list with duplicates removed, in the plugin's order; it never
    /// replaces one. Only `DO` and `PE` carry overrides.
    pub fn stream_mapparr(country: Option<&str>) -> Self {
        let mut map = Self::from_json(crate::data::STREAM_MAPPARR_ALIASES_JSON).unwrap_or_default();
        let Some(country) = country else {
            return map;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(
            crate::data::STREAM_MAPPARR_COUNTRY_ALIASES_JSON,
        ) else {
            return map;
        };
        let Some(overrides) = parsed
            .get(country.to_uppercase())
            .and_then(|value| value.as_object())
        else {
            return map;
        };
        for (key, value) in overrides {
            let Some(list) = value.as_array() else {
                continue;
            };
            if list.iter().any(|entry| !entry.is_string()) {
                continue;
            }
            let extra: Vec<String> = list
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string))
                .collect();
            let existing = map.entries.get(key).cloned().unwrap_or_default();
            let mut merged: Vec<String> = existing;
            for alias in extra {
                if !merged.contains(&alias) {
                    merged.push(alias);
                }
            }
            map.entries.insert(key.clone(), merged);
        }
        map
    }

    /// Parse a JSON object of `name -> [aliases]` into override entries, applying the
    /// plugin's validation: a value that is not an array of strings is skipped whole, and
    /// an empty array is kept so it can disable a built-in entry.
    pub fn overrides_from_json(json: &str) -> Option<HashMap<String, Vec<String>>> {
        let value: serde_json::Value = serde_json::from_str(json).ok()?;
        let object = value.as_object()?;
        let mut overrides = HashMap::with_capacity(object.len());
        for (key, value) in object {
            let Some(list) = value.as_array() else {
                continue;
            };
            if list.iter().any(|entry| !entry.is_string()) {
                continue;
            }
            overrides.insert(
                key.clone(),
                list.iter()
                    .filter_map(|entry| entry.as_str().map(str::to_string))
                    .collect(),
            );
        }
        Some(overrides)
    }

    /// Parse a JSON object of `name -> [aliases]`.
    ///
    /// Entries whose value is not an array of strings are skipped rather than failing the
    /// whole table, which is what the plugin does with malformed custom aliases.
    pub fn from_json(json: &str) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(json).ok()?;
        let object = value.as_object()?;
        let mut entries = HashMap::with_capacity(object.len());
        for (key, value) in object {
            let Some(list) = value.as_array() else {
                continue;
            };
            // A list with any non-string element is skipped whole, as the plugin does. An
            // empty list is kept: in a custom override it turns the built-in entry off.
            if list.iter().any(|entry| !entry.is_string()) {
                continue;
            }
            let aliases: Vec<String> = list
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string))
                .collect();
            entries.insert(key.clone(), aliases);
        }
        Some(Self { entries })
    }

    /// Merge custom aliases on top of the built-ins, replacing per key.
    ///
    /// An empty list is preserved rather than ignored: the plugin assigns the list
    /// outright, so a custom entry with no aliases disables the built-in one for that key.
    pub fn with_overrides(mut self, overrides: HashMap<String, Vec<String>>) -> Self {
        for (key, aliases) in overrides {
            self.entries.insert(key, aliases);
        }
        self
    }

    /// Aliases for one canonical name. The key is trimmed first: real Dispatcharr data
    /// carries trailing whitespace (`"NHL Network "`), which used to miss the entry.
    ///
    /// This is EPG Janitor's lookup, which trims. Stream-Mapparr's `alias_lookup` looks
    /// the name up VERBATIM first and only then through its folded index — see
    /// [`AliasMap::aliases_for_exact`].
    pub fn aliases_for(&self, name: &str) -> &[String] {
        self.entries
            .get(crate::pyre::py_trim(name))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// Aliases for a name, with no trimming: the plugin's literal `alias_map.get(name)`.
    pub fn aliases_for_exact(&self, name: &str) -> &[String] {
        self.entries
            .get(name)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// A case-folded view of the table, for Stream-Mapparr's folded-key fallback.
    ///
    /// Python builds this once per map, first folded key winning in the map's insertion
    /// order. Our tables parse out of JSON objects, which `serde_json` keeps in key
    /// order, so that order is used here — deterministic, and the shipped Stream-Mapparr
    /// table's only two folded collisions (`GOLF Channel`/`Golf Channel`,
    /// `Nat Geo WILD`/`Nat Geo Wild`) carry identical variants, so the choice cannot be
    /// observed.
    pub fn folded_index(&self) -> HashMap<String, Vec<String>> {
        let mut keys: Vec<&String> = self.entries.keys().collect();
        keys.sort();
        let mut index = HashMap::with_capacity(keys.len());
        for key in keys {
            index
                .entry(fold_alias_key(key))
                .or_insert_with(|| self.entries[key].clone());
        }
        index
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Case- and whitespace-insensitive form of an alias key.
///
/// `casefold` rather than `lower`, because channel names carry non-ASCII text and
/// casefold is the correct comparison for it; surrounding whitespace is stripped because
/// a hand-edited JSON entry leaves stray spaces. Both are the plugin's rules.
pub fn fold_alias_key(name: &str) -> String {
    crate::pyre::py_casefold(crate::pyre::py_trim(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_table_parses() {
        let map = AliasMap::builtin();
        assert!(map.len() > 200, "got {}", map.len());
        assert!(!map.aliases_for("ESPN").is_empty());
    }

    #[test]
    fn lookup_trims_the_key() {
        let map = AliasMap::builtin();
        assert_eq!(
            map.aliases_for("NHL Network"),
            map.aliases_for("NHL Network ")
        );
    }

    #[test]
    fn unknown_key_has_no_aliases() {
        assert!(AliasMap::builtin()
            .aliases_for("No Such Channel")
            .is_empty());
    }

    #[test]
    fn overrides_replace_rather_than_extend() {
        let mut custom = HashMap::new();
        custom.insert("ESPN".to_string(), vec!["Only This".to_string()]);
        custom.insert("Brand New".to_string(), vec!["BN".to_string()]);
        let map = AliasMap::builtin().with_overrides(custom);

        assert_eq!(map.aliases_for("ESPN"), ["Only This".to_string()]);
        assert_eq!(map.aliases_for("Brand New"), ["BN".to_string()]);
    }

    #[test]
    fn malformed_entries_are_skipped_not_fatal() {
        let map =
            AliasMap::from_json(r#"{"A": ["a"], "B": "not a list", "C": []}"#).expect("parses");
        assert_eq!(map.aliases_for("A"), ["a".to_string()]);
        assert!(map.aliases_for("B").is_empty());
        assert!(map.aliases_for("C").is_empty());
    }
}

#[cfg(test)]
mod stream_mapparr_tests {
    use super::*;

    #[test]
    fn stream_mapparr_ships_its_own_table() {
        let map = AliasMap::stream_mapparr(None);
        assert_eq!(map.len(), 198);
        // A key EPG Janitor does not ship at all.
        assert!(!map.aliases_for("Antena 3").is_empty());
        // A key both ship with different variants.
        assert!(map
            .aliases_for("NHL Network")
            .contains(&"NHL Net".to_string()));
        assert!(!map
            .aliases_for("NHL Network")
            .contains(&"NHL Channel".to_string()));
        // The other direction: a variant only EPG Janitor ships.
        assert!(AliasMap::builtin()
            .aliases_for("NHL Network")
            .contains(&"NHL Channel".to_string()));
    }

    #[test]
    fn country_overrides_extend_without_duplicates() {
        let base = AliasMap::stream_mapparr(None);
        let dominican = AliasMap::stream_mapparr(Some("DO"));
        assert!(dominican.len() > base.len());
        for (key, variants) in dominican.entries.iter() {
            assert_eq!(
                variants.len(),
                variants
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                "{key} has duplicates"
            );
        }
        // Case-insensitive code, and an unknown country is a no-op.
        assert_eq!(AliasMap::stream_mapparr(Some("do")).len(), dominican.len());
        assert_eq!(AliasMap::stream_mapparr(Some("ZZ")).len(), base.len());
    }

    #[test]
    fn folded_lookup_ignores_case_and_edges() {
        let map = AliasMap::stream_mapparr(None);
        let index = map.folded_index();
        assert_eq!(
            index.get(&fold_alias_key("  golf channel ")),
            index.get(&fold_alias_key("GOLF Channel"))
        );
        assert!(index.contains_key(&fold_alias_key("CNN En Español")));
    }

    #[test]
    fn exact_lookup_does_not_trim() {
        let map = AliasMap::builtin();
        assert!(!map.aliases_for_exact("NHL Network ").is_empty() || true);
        assert!(map.aliases_for_exact("NHL Network ").is_empty());
        assert!(!map.aliases_for_exact("NHL Network").is_empty());
    }
}
