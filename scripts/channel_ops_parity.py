#!/usr/bin/env python3
"""Parity harness: `crosstalk_core::channel_ops` against the EPG Janitor plugin.

The five cleanup actions in `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/plugin.py`
are each a channel query plus a bulk write. crosstalk replaces them with one primitive
(selection -> operations -> plan -> batched apply), so this harness runs both over the SAME
fixture world and compares:

  * the channel ids each side selects, and
  * the payload each side would write, per channel and per field.

Two `scope-narrows/*` cases assert the deliberate scope divergence instead of comparing:
the Rust selection must be a STRICT subset of the plugin's, with identical payloads on
the channels it keeps.

The plugin's logic is REPRODUCED here, not imported: `plugin.py` needs Django, Dispatcharr's
`apps.*` models and a live database. What is copied is the code that decides, verbatim where
it is not ORM, with the ORM calls replaced by lookups into the fixture:

  * `Plugin._validate_and_filter_groups`                                 plugin.py:1694-1828
  * `Plugin.remove_epg_assignments_action`                               plugin.py:2427-2473
  * `Plugin.add_bad_epg_suffix_action`                                   plugin.py:2474-2559
  * `Plugin.remove_epg_from_hidden_action`                               plugin.py:2562-2716
  * `Plugin.remove_epg_by_regex_action`                                  plugin.py:2915-2966
  * `Plugin.remove_all_epg_from_groups_action`                           plugin.py:2968-3034
  * the scan that produces the result set the first two read            plugin.py:2294-2395
  * `Plugin._bulk_update_channels`                                       plugin.py:1886-1908

Group tokens go through the plugin's own `wildcard_match.expand_patterns`, which is
Django-free, so the wildcard semantics compared here are the shipped ones.

Expected divergence, deliberately not a mismatch: `remove_epg_from_hidden` also DELETES the
`ProgramData` rows of each entry (`plugin.py:2619-2621`), which blanks the guide of every
visible channel sharing that entry. crosstalk only unassigns, so this harness compares the
`epg_data = None` payload and notes the deletion instead of reproducing it.

Usage:
    python3 -I scripts/channel_ops_parity.py [--show N]

Exit status is 1 when a case mismatches, so this gates CI.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

# Importing the plugin's own wildcard module must not leave __pycache__ in the reference
# checkout. `python3 -I` ignores PYTHONDONTWRITEBYTECODE, so set it here, before the import.
sys.dont_write_bytecode = True

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGIN_INNER = os.path.join(REPO, "reference", "Dispatcharr-EPG-Janitor-Plugin", "EPG-Janitor")
BINARY = os.path.join(REPO, "target", "debug", "examples", "channel_ops_io")
EXAMPLES = ["build", "-q", "-p", "crosstalk-core", "--example", "channel_ops_io"]

# A Python-only construct: `re` accepts a backreference, the Rust `regex` engine does not.
# The harness checks that crosstalk REFUSES it rather than matching something else.
SYNTAX_GAP_PATTERN = r"(es)pn\1"

# ---------------------------------------------------------------------------
# The fixture world.
#
# Deliberately awkward: unicode names, a name that already carries the bad-EPG suffix, an
# entry shared by two channels, regex metacharacters in both a channel and a guide name,
# integer/string/missing channel numbers, and profiles whose memberships make some channels
# visible in one and hidden in another. `hidden_from_output` (channel 10) is carried because
# it LOOKS like the same concept: "hidden" in `remove_epg_from_hidden` means a DISABLED
# profile membership, never this flag, and the harness keeps the distinction honest.
# ---------------------------------------------------------------------------
CHANNELS = [
    # id, name, number, group, epg, hidden_from_output
    (1, "ESPN", 101, 10, 31, False),
    (2, "CNN", "102", 10, 32, False),
    (3, "CNN [BadEPG]", 103, 10, 33, False),
    (4, "ESPN (HD) [US]", 104, 10, 31, False),
    (5, "Télé-Québec", 201, 20, 34, False),
    (6, "日本 NHK", None, 20, 35, False),
    (7, "Canal 5 | US", "207", 20, None, False),
    (8, "A+B*C.D?", 301, 30, 36, False),
    (9, "US: HBO (East)", 302, 30, 38, False),
    (10, "TNT [US]", 303, 10, 37, True),
    (11, "PPV 1", 401, 40, None, False),
    (12, "Local 7", None, None, None, False),
    (13, "Español Ñ", 105, 11, 39, False),
    (14, "Telemundo", 202, 20, 40, False),
]

GROUPS = [
    (10, "Sports"),
    (11, "Sports Extra"),
    (20, "Latino"),
    (30, "US|Intl"),
    (40, "PPV Live"),
]

# profile id -> (name, enabled channel ids). A channel absent from the list is disabled in
# that profile, which is how Dispatcharr treats it too (the REST serializer only lists the
# enabled ones).
PROFILES = [
    (1, "All", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14]),
    (2, "Favorites", [1, 5, 8, 13]),
    (3, "Sports", [1, 2, 3, 4, 10, 13]),
    (4, "Latino", [5, 6, 7, 9, 14]),
]

ENTRIES = [
    (31, "ESPN HD"),
    (32, "CNN [US]"),
    (33, "CNN [BadEPG HD]"),
    (34, "Télé-Québec HD"),
    (35, "NHK World"),
    (36, "A+B*C.D Guide"),
    (37, "TNT [US]"),
    (38, "US: HBO (East)"),
    (39, "Español Ñ Guide"),
    (40, "Telemundo Este"),
]

# EPG entries that HAVE programme data in the window. The scan reports the others, which is
# what `add_bad_epg_suffix` and `remove_epg_assignments` read (plugin.py:2320-2390).
EPG_IDS_WITH_PROGRAMS = {31, 34, 36}

# The settings variants both sides are run under. `channel_profile_name`, `selected_groups`
# and `ignore_groups` are the plugin's own setting names; in crosstalk they are `[scope]`.
SETTING_VARIANTS = [
    ("default", {}),
    ("profile-all", {"channel_profile_name": "All"}),
    ("profile-sports", {"channel_profile_name": "Sports"}),
    ("groups-sports", {"selected_groups": "Sports"}),
    ("groups-wildcard", {"selected_groups": "Spo*"}),
    ("ignore-ppv", {"ignore_groups": "PPV*"}),
    ("profiles-multi", {"channel_profile_name": "Favorites, Latino"}),
]

# Cases where both sides must REFUSE rather than select: the plugin raises ValueError from
# its own validation, crosstalk returns an error from `scope_channels`.
ERROR_VARIANTS = [
    ("both-group-settings", {"selected_groups": "Sports", "ignore_groups": "PPV*"}, "remove_all_epg_from_groups"),
    ("group-case-mismatch", {"selected_groups": "sports"}, "remove_all_epg_from_groups"),
    ("unknown-profile", {"channel_profile_name": "Nope"}, "remove_all_epg_from_groups"),
    ("unknown-hidden-profile", {"channel_profile_name": "Nope"}, "remove_epg_from_hidden"),
    ("blank-regex", {"epg_regex_to_remove": "  "}, "remove_epg_by_regex"),
]

SUFFIXES = [" [BadEPG]", " ★"]

REGEXES = [
    "US",  # case-sensitive: matches the guide names, not the lowercased ones
    "cnn",  # …and therefore matches nothing
    "^US:",
    "Ñ",
    ".*",
    r"\[.*\]",
    "Telemundo|Español",
]


class World:
    """The fixture, with the ORM queries the plugin's actions make."""

    def __init__(self) -> None:
        self.channels = [
            {
                "id": cid,
                "name": name,
                "channel_number": number,
                "channel_group_id": group,
                "epg_data_id": epg,
                "hidden_from_output": hidden,
            }
            for cid, name, number, group, epg, hidden in CHANNELS
        ]
        self.groups = [{"id": gid, "name": name} for gid, name in GROUPS]
        self.profiles = [{"id": pid, "name": name, "channels": list(ids)} for pid, name, ids in PROFILES]
        self.entries = [{"id": eid, "name": name} for eid, name in ENTRIES]

    def channel(self, channel_id: int) -> dict:
        return next(channel for channel in self.channels if channel["id"] == channel_id)

    def entry(self, entry_id: int | None) -> dict | None:
        if entry_id is None:
            return None
        return next((entry for entry in self.entries if entry["id"] == entry_id), None)

    def fixture(self) -> dict:
        return {
            "channels": self.channels,
            "groups": self.groups,
            "profiles": self.profiles,
            "entries": self.entries,
        }

    # -- the plugin's queries, over the fixture -----------------------------

    def channels_with_epg(self, ids: list[int] | None = None) -> list[dict]:
        """`Channel.objects.filter(epg_data__isnull=False)` (plugin.py:2939, 2971)."""
        return [
            channel
            for channel in self.channels
            if channel["epg_data_id"] is not None and (ids is None or channel["id"] in ids)
        ]

    def profiles_by_name(self) -> dict[str, dict]:
        """`ChannelProfile.objects.all()` with the case-insensitive name map (1724-1727)."""
        return {profile["name"].strip().upper(): profile for profile in self.profiles}

    def filter_groups(self, settings: dict) -> tuple[list[dict], str]:
        """`Plugin._validate_and_filter_groups`, plugin.py:1694-1828.

        Reimplemented over the fixture: the two queries are
        `channels_query.filter(channel_group_id__in=...)` and `.exclude(...)`, and the
        profile filter is `ChannelProfileMembership.objects.filter(enabled=True)`.
        """
        selected_groups_str = settings.get("selected_groups", "").strip()
        ignore_groups_str = settings.get("ignore_groups", "").strip()
        channel_profile_names_str = settings.get("channel_profile_name", "").strip()

        # Validation: both cannot be used together (1701-1702).
        if selected_groups_str and ignore_groups_str:
            raise ValueError(
                "Cannot use both 'Channel Groups' and 'Ignore Groups' at the same time. "
                "Please use only one."
            )

        group_name_to_id = {group["name"]: group["id"] for group in self.groups}
        channels = list(self.channels)
        group_filter_info = ""
        profile_filter_info = ""

        if channel_profile_names_str:
            profile_names = [
                name.strip() for name in re.split(r"[,\n]+", channel_profile_names_str) if name.strip()
            ]
            profile_name_to_id = self.profiles_by_name()
            all_visible_channel_ids: set[int] = set()
            found_profiles: list[str] = []
            not_found_profiles: list[str] = []

            for profile_name in profile_names:
                profile = profile_name_to_id.get(profile_name.upper())
                if not profile:
                    not_found_profiles.append(profile_name)
                    continue
                visible = list(profile["channels"])
                if visible:
                    all_visible_channel_ids.update(visible)
                    found_profiles.append(profile_name)

            if not all_visible_channel_ids:
                if not_found_profiles and not found_profiles:
                    available = ", ".join(p["name"] for p in self.profiles if p["name"])
                    raise ValueError(
                        "None of the specified Channel Profiles were found: "
                        f"{', '.join(profile_names)}. Available profiles: {available}"
                    )
                raise ValueError(
                    "The specified Channel Profile(s) have no visible channels: "
                    f"{', '.join(found_profiles if found_profiles else profile_names)}"
                )

            channels = [c for c in channels if c["id"] in all_visible_channel_ids]
            if len(found_profiles) == 1:
                profile_filter_info = f" in profile '{found_profiles[0]}'"
            else:
                profile_filter_info = " in profiles " + ", ".join(f"'{n}'" for n in found_profiles)

        if selected_groups_str:
            selected_groups = [g.strip() for g in re.split(r"[,\n]+", selected_groups_str) if g.strip()]
            matched_groups, _ = wildcard_match.expand_patterns(
                selected_groups, list(group_name_to_id), ci_plain=False
            )
            valid_group_ids = [group_name_to_id[name] for name in matched_groups]
            if not valid_group_ids:
                raise ValueError(
                    f"None of the specified groups were found: {', '.join(selected_groups)}"
                )
            channels = [c for c in channels if c["channel_group_id"] in valid_group_ids]
            group_filter_info = " in groups " + ", ".join(f"'{n}'" for n in matched_groups)
        elif ignore_groups_str:
            ignore_groups = [g.strip() for g in re.split(r"[,\n]+", ignore_groups_str) if g.strip()]
            matched_ignore, _ = wildcard_match.expand_patterns(
                ignore_groups, list(group_name_to_id), ci_plain=False
            )
            ignore_group_ids = [group_name_to_id[name] for name in matched_ignore]
            if ignore_group_ids:
                channels = [c for c in channels if c["channel_group_id"] not in ignore_group_ids]
            resolved = matched_ignore or ignore_groups
            group_filter_info = " (ignoring groups " + ", ".join(f"'{n}'" for n in resolved) + ")"

        return channels, profile_filter_info + group_filter_info

    def scan_rows(self, settings: dict | None = None) -> list[dict]:
        """The last scan's result set, which the first two actions read.

        `_scan_missing_epg_worker` (plugin.py:2294-2395) takes "channels with EPG" in the
        scope (`_validate_and_filter_groups`, 2308-2314), so the group/profile filter has
        ALREADY been applied when the rows are stored; each row records the channel and the
        guide it was found on. Passing `settings` reproduces that filter, which is what keeps
        a cleanup run's selection in the same scope as the scan that fed it.
        """
        if settings is None:
            scoped = list(self.channels)
        else:
            scoped, _ = self.filter_groups(settings)
        in_scope = {channel["id"] for channel in scoped}
        return [
            {"channel_id": channel["id"], "epg_data_id": channel["epg_data_id"]}
            for channel in self.channels_with_epg()
            if channel["epg_data_id"] not in EPG_IDS_WITH_PROGRAMS
            and channel["id"] in in_scope
        ]


# The plugin's own wildcard resolver. Django-free by design, so the glob semantics compared
# here are the shipped ones (`fnmatch`, i.e. `*`, `?` and `[seq]`; crosstalk's port supports
# `*` and `?`, and no case below uses a bracket class).
if os.path.isdir(PLUGIN_INNER):
    sys.path.insert(0, PLUGIN_INNER)
    import wildcard_match  # noqa: E402
else:  # pragma: no cover - the parity job checks the submodule out
    print(
        f"reference plugin not found at {PLUGIN_INNER}; run `git submodule update --init`",
        file=sys.stderr,
    )
    raise SystemExit(2)


# ---------------------------------------------------------------------------
# The plugin's five actions: selection and payload, from plugin.py.
# ---------------------------------------------------------------------------
def action_add_bad_epg_suffix(world: World, settings: dict) -> tuple[list[int], dict]:
    """`add_bad_epg_suffix_action`, plugin.py:2474-2559."""
    bad_epg_suffix = settings.get("bad_epg_suffix", " [BadEPG]")
    remove_epg_enabled = settings.get("remove_epg_with_suffix", False)

    channels_with_missing_data = world.scan_rows(settings)
    channel_ids = [ch["channel_id"] for ch in channels_with_missing_data]
    # "Get current channel names via ORM to ensure we have the latest data" (2501-2504).
    channel_id_to_name = {c["id"]: c["name"] for c in world.channels if c["id"] in channel_ids}

    selected = list(channel_ids)
    payload = []
    for channel in channels_with_missing_data:
        channel_id = channel["channel_id"]
        current_name = channel_id_to_name.get(channel_id, "")
        # "Only add suffix if it is not already present" (2508-2510).
        if not current_name.endswith(bad_epg_suffix):
            update = {"id": channel_id, "name": f"{current_name}{bad_epg_suffix}"}
            if remove_epg_enabled:
                update["epg_data_id"] = None
            payload.append(update)
    return selected, {"rows": payload}


def action_remove_epg_assignments(world: World, settings: dict) -> tuple[list[int], dict]:
    """`remove_epg_assignments_action`, plugin.py:2427-2473."""
    channel_ids = [ch["channel_id"] for ch in world.scan_rows(settings)]
    # Every stored id is written, with no re-check of the channel's current state (2452-2457).
    return channel_ids, {"rows": [{"id": cid, "epg_data_id": None} for cid in channel_ids]}


def action_remove_epg_from_hidden(world: World, settings: dict) -> tuple[list[int], dict]:
    """`remove_epg_from_hidden_action`, plugin.py:2562-2716.

    The profile list is `channel_profile_name` (2574-2588); the selection is the DISABLED
    membership rows of those profiles. The payload is `epg_data = None` for the hidden
    channels that have a guide (2640-2646). The plugin ALSO deletes the ProgramData rows of
    each entry (2619-2621); crosstalk does not, which is the documented divergence.
    """
    channel_profile_name = settings.get("channel_profile_name", "").strip()
    if not channel_profile_name:
        raise ValueError("Channel Profile Name is required. Please configure it in settings.")

    profile_names = [p.strip() for p in re.split(r"[,\n]+", channel_profile_name) if p.strip()]
    profile_id_to_name = {p["name"]: p["id"] for p in world.profiles}
    profile_ids = []
    for name in profile_names:
        if name not in profile_id_to_name:
            raise ValueError(f"Channel profile '{name}' not found")
        profile_ids.append(profile_id_to_name[name])

    enabled_by_profile = {p["id"]: set(p["channels"]) for p in world.profiles}
    hidden: list[dict] = []
    for profile_id in profile_ids:
        for channel in world.channels:
            if channel["id"] not in enabled_by_profile[profile_id] and channel not in hidden:
                hidden.append(channel)

    selected = [channel["id"] for channel in hidden]
    # `if channel.epg_data: … channels_to_clear.append(channel)` (2614-2622), then one
    # `Channel.objects.filter(id__in=…).update(epg_data=None)` (2642-2645).
    payload = [{"id": c["id"], "epg_data_id": None} for c in hidden if c["epg_data_id"] is not None]
    return selected, {"rows": payload}


def action_remove_epg_by_regex(world: World, settings: dict) -> tuple[list[int], dict]:
    """`remove_epg_by_regex_action`, plugin.py:2915-2966."""
    pattern = settings.get("epg_regex_to_remove", "").strip()
    if not pattern:
        raise ValueError("Please provide a REGEX pattern in the settings.")
    compiled = re.compile(pattern)  # no flags: case-SENSITIVE (2923)

    channels_query, _ = world.filter_groups(settings)
    with_epg = [c for c in channels_query if c["epg_data_id"] is not None]

    selected = []
    for channel in with_epg:
        entry = world.entry(channel["epg_data_id"])
        epg_name = entry["name"] if entry else ""
        # `.search`, not `.match`/`.fullmatch` (2943).
        if epg_name and compiled.search(epg_name):
            selected.append(channel["id"])

    return selected, {"rows": [{"id": cid, "epg_data_id": None} for cid in selected]}


def action_remove_all_epg_from_groups(world: World, settings: dict) -> tuple[list[int], dict]:
    """`remove_all_epg_from_groups_action`, plugin.py:2968-3034."""
    channels_query, _ = world.filter_groups(settings)
    with_epg = [c for c in channels_query if c["epg_data_id"] is not None]
    selected = [c["id"] for c in with_epg]
    return selected, {"rows": [{"id": cid, "epg_data_id": None} for cid in selected]}


ACTIONS = {
    "add_bad_epg_suffix": action_add_bad_epg_suffix,
    "remove_epg_assignments": action_remove_epg_assignments,
    "remove_epg_from_hidden": action_remove_epg_from_hidden,
    "remove_epg_by_regex": action_remove_epg_by_regex,
    "remove_all_epg_from_groups": action_remove_all_epg_from_groups,
}


def normalise_payload(payload: dict) -> dict[str, dict[str, str]]:
    """`{channel_id: {field: wire_value}}`, the shape the Rust side answers.

    The plugin's payloads are `{id, name}` / `{id, epg_data_id}` dicts; `_bulk_update_channels`
    (1904-1907) applies exactly those fields, so the comparison is per field.
    """
    out: dict[str, dict[str, str]] = {}
    for row in payload["rows"]:
        channel_id = str(row["id"])
        for field, value in row.items():
            if field == "id":
                continue
            wire = "none" if value is None else str(value)
            out.setdefault(channel_id, {})[field] = wire
    return out


def normalise_changes(answer: dict) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for change in answer["changes"]:
        out.setdefault(str(change["channel_id"]), {})[change["field"]] = change["after_wire"]
    return out


# ---------------------------------------------------------------------------
# The cases.
# ---------------------------------------------------------------------------
def scope_selection(settings: dict) -> dict:
    return {
        "profiles": settings.get("channel_profile_name", ""),
        "groups": settings.get("selected_groups", ""),
        "ignore_groups": settings.get("ignore_groups", ""),
    }


def build_cases(world: World) -> tuple[list[dict], list[dict]]:
    """The Rust cases (what `channel_ops_io` runs) and the Python expectations."""
    rust_cases: list[dict] = []
    expectations: list[dict] = []

    for label, settings in SETTING_VARIANTS:
        scope = scope_selection(settings)
        scan_rows = world.scan_rows(settings)
        for action in ACTIONS:
            # `remove_epg_from_hidden` needs the profile setting (2574-2578) and
            # `remove_epg_by_regex` needs a pattern; both get their own matrix below.
            if action == "remove_epg_from_hidden" and not settings.get("channel_profile_name"):
                continue
            if action == "remove_epg_by_regex":
                continue
            try:
                selected, payload = ACTIONS[action](world, settings)
            except ValueError as error:
                selected, payload = None, {"error": str(error)}

            if action in ("add_bad_epg_suffix", "remove_epg_assignments"):
                selection = dict(scope)
                selection["channel_ids"] = [row["channel_id"] for row in scan_rows]
                selection["expected_epg"] = {
                    str(row["channel_id"]): row["epg_data_id"] for row in scan_rows
                }
            elif action == "remove_epg_from_hidden":
                selection = {
                    "groups": scope["groups"],
                    "ignore_groups": scope["ignore_groups"],
                    "hidden_in_profiles": settings["channel_profile_name"],
                }
            elif action == "remove_epg_by_regex":
                selection = dict(scope)
                selection["epg_name_regex"] = settings["epg_regex_to_remove"]
            else:
                selection = dict(scope)
                selection["has_epg"] = True

            if action == "add_bad_epg_suffix":
                ops = [{"op": "rename", "kind": {"suffix": settings.get("bad_epg_suffix", " [BadEPG]")}}]
                if settings.get("remove_epg_with_suffix"):
                    ops.append({"op": "clear_epg"})
                name = f"{label}/{action}"
            elif action == "remove_epg_assignments":
                ops = [{"op": "clear_epg"}]
                name = f"{label}/{action}"
            elif action == "remove_epg_by_regex":
                ops = [{"op": "clear_epg"}]
                name = f"{label}/{action}/{settings['epg_regex_to_remove']}"
            else:
                ops = [{"op": "clear_epg"}]
                name = f"{label}/{action}"

            rust_cases.append({"name": name, "selection": selection, "ops": ops})
            expectations.append(
                {
                    "name": name,
                    "selected": selected,
                    "payload": payload,
                    "note": "",
                }
            )

    # Suffix variants, including a name that already carries the suffix and a unicode suffix.
    for suffix in SUFFIXES:
        settings = {"bad_epg_suffix": suffix, "remove_epg_with_suffix": False}
        scan_rows = world.scan_rows(settings)
        selected, payload = action_add_bad_epg_suffix(world, settings)
        name = f"suffix/{suffix!r}"
        rust_cases.append(
            {
                "name": name,
                "selection": {
                    "channel_ids": [row["channel_id"] for row in scan_rows],
                    "expected_epg": {
                        str(row["channel_id"]): row["epg_data_id"] for row in scan_rows
                    },
                },
                "ops": [{"op": "rename", "kind": {"suffix": suffix}}],
            }
        )
        expectations.append({"name": name, "selected": selected, "payload": payload, "note": ""})

    # `remove_epg_with_suffix` combines the two operations, and the plugin couples them: it
    # skips a channel that already carries the suffix ENTIRELY, so that channel keeps its
    # guide (2508-2514). crosstalk matches that with the `suffix_present_skips_rest` plan
    # option (what the `tag_missing` preset sets); without it the operations stay
    # independent. Compared as parity, like everything else.
    settings = {"bad_epg_suffix": " [BadEPG]", "remove_epg_with_suffix": True}
    scan_rows = world.scan_rows(settings)
    selected, payload = action_add_bad_epg_suffix(world, settings)
    name = "combined/suffix-and-clear"
    rust_cases.append(
        {
            "name": name,
            "selection": {
                "channel_ids": [row["channel_id"] for row in scan_rows],
                "expected_epg": {
                    str(row["channel_id"]): row["epg_data_id"] for row in scan_rows
                },
            },
            "ops": [
                {"op": "rename", "kind": {"suffix": " [BadEPG]"}},
                {"op": "clear_epg"},
            ],
            "options": {"suffix_present_skips_rest": True},
        }
    )
    expectations.append({"name": name, "selected": selected, "payload": payload})

    # Regex variants, over every setting variant: selection and case sensitivity both matter.
    for label, settings in SETTING_VARIANTS:
        for pattern in REGEXES:
            case_settings = dict(settings)
            case_settings["epg_regex_to_remove"] = pattern
            selected, payload = action_remove_epg_by_regex(world, case_settings)
            name = f"regex/{label}/{pattern!r}"
            selection = scope_selection(settings)
            selection["epg_name_regex"] = pattern
            rust_cases.append(
                {"name": name, "selection": selection, "ops": [{"op": "clear_epg"}]}
            )
            expectations.append({"name": name, "selected": selected, "payload": payload, "note": ""})

    # Refusals: the plugin raises from its own validation, crosstalk returns an error.
    for label, settings, action in ERROR_VARIANTS:
        try:
            selected, payload = ACTIONS[action](world, settings)
            error = None
        except ValueError as raised:
            selected, payload, error = None, None, str(raised)
        name = f"refusal/{label}"
        selection = scope_selection(settings)
        selection.pop("profiles")
        if action == "remove_epg_from_hidden":
            selection["hidden_in_profiles"] = settings["channel_profile_name"]
        else:
            selection["profiles"] = settings.get("channel_profile_name", "")
            selection["has_epg"] = True
        if action == "remove_epg_by_regex":
            selection["epg_name_regex"] = settings["epg_regex_to_remove"]
        rust_cases.append(
            {
                "name": name,
                "selection": selection,
                "ops": [{"op": "clear_epg"}],
            }
        )
        expectations.append(
            {
                "name": name,
                "selected": selected,
                "payload": payload,
                "error": error,
                "note": "",
            }
        )

    # Deliberate divergence, locked in rather than compared: every crosstalk selection is
    # intersected with `[scope]`, while the plugin's hidden action ignores the group
    # settings and its scan-based actions apply the stored ids unchecked. The Rust side must
    # therefore select a STRICT subset of the plugin's channels, with an identical payload on
    # every channel it does select. These cases assert exactly that (`subset: True`).
    hidden_selected, hidden_payload = action_remove_epg_from_hidden(
        world, {"channel_profile_name": "Favorites"}
    )
    name = "scope-narrows/hidden-in-favorites-groups-latino"
    rust_cases.append(
        {
            "name": name,
            "selection": {
                "groups": "Latino",
                "hidden_in_profiles": "Favorites",
            },
            "ops": [{"op": "clear_epg"}],
        }
    )
    expectations.append(
        {"name": name, "selected": hidden_selected, "payload": hidden_payload, "subset": True}
    )

    full_scan = world.scan_rows({})
    narrowed_selected, narrowed_payload = action_remove_epg_assignments(world, {})
    name = "scope-narrows/scan-ids-groups-sports"
    rust_cases.append(
        {
            "name": name,
            "selection": {
                "groups": "Sports",
                "channel_ids": [row["channel_id"] for row in full_scan],
                "expected_epg": {
                    str(row["channel_id"]): row["epg_data_id"] for row in full_scan
                },
            },
            "ops": [{"op": "clear_epg"}],
        }
    )
    expectations.append(
        {
            "name": name,
            "selected": narrowed_selected,
            "payload": narrowed_payload,
            "subset": True,
        }
    )

    # The syntax gap: Python accepts a backreference, the Rust engine does not. The expected
    # outcome is a REFUSAL on the Rust side, so this is reported, not counted as a mismatch.
    python_selected, _ = action_remove_epg_by_regex(
        world, {"epg_regex_to_remove": SYNTAX_GAP_PATTERN}
    )
    name = f"python-only/{SYNTAX_GAP_PATTERN!r}"
    rust_cases.append(
        {
            "name": name,
            "selection": {"epg_name_regex": SYNTAX_GAP_PATTERN},
            "ops": [{"op": "clear_epg"}],
        }
    )
    expectations.append(
        {
            "name": name,
            "selected": python_selected,
            "payload": None,
            "error": None,
            "note": "expected divergence: Python's `re` accepts a backreference, the Rust "
            "`regex` engine rejects it and crosstalk reports the gap",
            "expect_rust_error": True,
        }
    )

    return rust_cases, expectations


def run_rust(cases: list[dict], world: World) -> list[dict]:
    document = {"world": world.fixture(), "cases": cases}
    with tempfile.TemporaryDirectory(prefix="crosstalk-channel-ops-") as work:
        path = os.path.join(work, "fixture.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(document, handle)
        completed = subprocess.run(
            [BINARY, path], capture_output=True, text=True, check=True
        )
    answers = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(answers) != len(cases):
        raise SystemExit(f"Rust produced {len(answers)} answers for {len(cases)} cases")
    return answers


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show", type=int, default=10, help="mismatches to print in full")
    args = parser.parse_args()

    if not os.path.isdir(PLUGIN_INNER):
        print(
            f"reference plugin not found at {PLUGIN_INNER}; "
            "run `git submodule update --init` to check it out",
            file=sys.stderr,
        )
        return 2

    # Always rebuild: a stale binary would be silently compared.
    print(f"building {BINARY} ...", file=sys.stderr)
    subprocess.run(["cargo", *EXAMPLES], cwd=REPO, check=True)

    world = World()
    cases, expectations = build_cases(world)
    answers = run_rust(cases, world)

    mismatches = []
    noted = []
    refusals = 0
    subsets = 0
    for expected, answer in zip(expectations, answers):
        if expected.get("note"):
            noted.append(expected)
            if expected.get("expect_rust_error") and "error" not in answer:
                mismatches.append((expected, answer, "expected the Rust side to refuse"))
            continue

        if expected.get("subset"):
            # The documented scope divergence: fewer channels, same payloads where set.
            if "error" in answer:
                mismatches.append((expected, answer, f"Rust refused: {answer['error']}"))
                continue
            rust_selected = set(answer["selected"])
            plugin_selected = set(expected["selected"])
            if not rust_selected < plugin_selected:
                mismatches.append(
                    (
                        expected,
                        answer,
                        "not a strict subset: "
                        f"plugin={sorted(plugin_selected)} rust={sorted(rust_selected)}",
                    )
                )
                continue
            plugin_payload = normalise_payload(expected["payload"])
            rust_payload = normalise_changes(answer)
            differing = [
                cid
                for cid in rust_selected
                if plugin_payload.get(str(cid), {}) != rust_payload.get(str(cid), {})
            ]
            if differing:
                mismatches.append(
                    (expected, answer, f"payload differs on {sorted(differing)}")
                )
            else:
                subsets += 1
            continue

        if "error" in answer:
            # A refusal is only right when the plugin refused too.
            if expected.get("error") is None:
                mismatches.append((expected, answer, f"Rust refused: {answer['error']}"))
            else:
                refusals += 1
            continue
        if expected.get("error") is not None:
            mismatches.append(
                (expected, answer, f"Rust accepted, plugin refused: {expected['error']}")
            )
            continue

        rust_selected = sorted(answer["selected"])
        if rust_selected != sorted(expected["selected"]):
            mismatches.append(
                (
                    expected,
                    answer,
                    f"selection: plugin={sorted(expected['selected'])} rust={rust_selected}",
                )
            )
            continue

        plugin_payload = normalise_payload(expected["payload"])
        rust_payload = normalise_changes(answer)
        if plugin_payload != rust_payload:
            mismatches.append(
                (expected, answer, f"payload: plugin={plugin_payload} rust={rust_payload}")
            )

    print(
        f"[channel ops] {len(cases) - len(noted)} compared "
        f"({refusals} refusals agreed, {subsets} safer-subset agreed), "
        f"{len(noted)} noted, {len(mismatches)} mismatches"
    )
    for expected in noted:
        print(f"  noted: {expected['name']}: {expected['note']}")
    for expected, answer, why in mismatches[: args.show]:
        print(f"  {expected['name']}: {why}")
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")

    # Stated rather than compared: the plugin deletes programme rows, crosstalk unassigns.
    print(
        "  note: remove_epg_from_hidden also deletes ProgramData in the plugin "
        "(plugin.py:2619-2621); only the epg_data=None payload is compared"
    )
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
