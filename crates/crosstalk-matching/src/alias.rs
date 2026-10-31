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
    /// The shipped table from `data/aliases.json`.
    pub fn builtin() -> Self {
        Self::from_json(crate::data::ALIASES_JSON).unwrap_or_default()
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
    pub fn aliases_for(&self, name: &str) -> &[String] {
        self.entries
            .get(crate::pyre::py_trim(name))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
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
