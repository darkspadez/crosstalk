#!/usr/bin/env python3
"""Single-rule ablation on one fixed base matcher (plan step 5).

Whole-path oracles differ in many rules at once, so comparing Janitor's verdict
with Lineuparr's on a pair says which PLUGIN was right, not which RULE. This
module isolates rules instead: it builds one base engine (Lineuparr's
``match_all_streams`` over the shared core's ``normalize_name``, loaded in
Lineuparr's own ``oracle_lu`` namespace by ``oracles.py``) and swaps exactly the
rows under test into it, taking each variant from the plugin that ships it:

* table rows swap the module-level table the core reads at call time
  (``QUALITY_PATTERNS``, ``PROVIDER_PREFIX_PATTERNS``, ``GEOGRAPHIC_PATTERNS``,
  ``REGIONAL_PATTERNS``, ``MISC_PATTERNS``, ``_DECORATOR_CATS``) for the
  plugin's own list, imported from that plugin's ``oracle_<tag>`` module;
* step rows patch ONE statement of the core's ``normalize_name`` or of
  Lineuparr's ``match_all_streams`` source (each anchor must occur exactly once,
  so upstream drift fails loudly), replacing it with the plugin's statement
  (``&`` -> ``and``, letters-only dots, the suffix floor, the prefix-strip
  position, the ``Cf`` strip, the bare East/West strip);
* function rows bind the plugin's own function (``_has_token_overlap``,
  ``alias_match`` / ``alias_lookup``, the callsign extractor) onto the base;
* post-match rules (digit guards, zone rules) run as filters on the base's
  answer, written from the plugin source cited next to each.

Rows that interact are searched jointly (``GROUPS``: the zone words with the
zone filter, the prefix table with the prefix order and the geographic table,
the digit guards with designator repair, the whole callsign ladder), and the
whole configuration is settled by coordinate descent: every group is re-decided
with every other group at its current winner until nothing changes, so the
decisions are ONE engine, not 57 independent picks.

Scoring uses construction-truth labels (and AI labels after a ``--ai``
rerun). Unanimous and majority labels include the base plugin's own vote, so
on those pairs the unpatched base agrees with the label by construction; using
them would favour the base's variant.

``python3 -I scripts/corpus/ablate.py --self-test`` checks that every patch
anchor applies and that each patched variant changes the case it exists for.
"""
from __future__ import annotations

import argparse
import inspect
import json
import os
import re
import sys
import textwrap
import types

sys.dont_write_bytecode = True

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import oracles  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MERGED = os.path.join(REPO, "data", "merged")

#: The base engine runs at both cuts; a row is only ``strong`` when its winner is
#: the same at both (the decision must not hinge on the threshold, which step 6
#: sets separately on the corpus).
THRESHOLDS = (80, 90)

#: Cost of a false positive relative to a miss (a wrong attach/assign/rename is
#: worse than leaving a channel for the operator).
FP_WEIGHT = 2


def _mods():
    """The four plugins' modules, each from its own isolated namespace."""
    oracles.all_oracles()
    return {
        "core": sys.modules["oracle_lu.matching_core"],
        "lu": sys.modules["oracle_lu.fuzzy_matcher"],
        "janitor": sys.modules["oracle_janitor.fuzzy_matcher"],
        "sm": sys.modules["oracle_sm.fuzzy_matcher"],
        "cm": sys.modules["oracle_cm.fuzzy_matcher"],
    }


# ---------------------------------------------------------------------------
# source patches
# ---------------------------------------------------------------------------

def _patched_function(func, replacements, module):
    """Recompile ``func`` with textual ``replacements`` in ``module``'s globals.

    Each ``(old, new)`` must match exactly once in the dedented source, so a
    drifted upstream statement fails loudly instead of silently not patching.
    The function keeps the module's live globals, so table swaps still apply.
    """
    source = textwrap.dedent(inspect.getsource(func))
    for old, new in replacements:
        count = source.count(old)
        if count != 1:
            raise RuntimeError(
                f"ablation anchor for {func.__qualname__} matched {count} times: {old!r}")
        source = source.replace(old, new)
    namespace: dict = {}
    exec(compile(source, f"<ablate {func.__qualname__}>", "exec"),
         module.__dict__, namespace)
    return namespace[func.__name__]


_CORE_PREFIX_LOOP = (
    "    for pattern in PROVIDER_PREFIX_PATTERNS:\n"
    "        name = re.sub(pattern, '', name, flags=re.IGNORECASE)\n")
_CORE_LEADING_PAREN_END = (
    "        if new_name == name:\n"
    "            break\n"
    "        name = new_name\n")
_CORE_DOTS = "    name = re.sub(r'(?<=\\w)\\.(?=\\w)', ' ', name)\n"
_CORE_CF = "    name = ''.join(c for c in name if unicodedata.category(c) != 'Cf')\n"
_CORE_EW = (
    "    if ignore_regional:\n"
    "        name = re.sub(r'\\(\\s*(?:eastern|east|e|west|w)\\s*\\)', ' ', name, flags=re.IGNORECASE)\n"
    "        name = re.sub(r'\\b(?:eastern|east|west)\\b', ' ', name, flags=re.IGNORECASE)\n")
_CORE_SUFFIX = (
    "    name = re.sub(r'\\s+Network\\s*$', '', name, flags=re.IGNORECASE)\n"
    "    name = re.sub(r'\\s+Channel\\s*$', '', name, flags=re.IGNORECASE)\n"
    "    name = re.sub(r'\\s+TV\\s*$', '', name, flags=re.IGNORECASE)\n")
_CORE_CAMEL_END = "    name = re.sub(r'([a-z]{4,})([A-Z]{2,})\\b', r'\\1 \\2', name)\n"

#: normalize_name statement patches, keyed by (row, variant).
NORMALIZE_PATCHES = {
    # JAN fuzzy_matcher.py:632-658 / CM :817-854: prefixes stripped after the
    # hyphen -> space step and after the leading-paren strip.
    ("N12", "after-hyphen"): [(_CORE_PREFIX_LOOP, ""),
                              (_CORE_LEADING_PAREN_END,
                               _CORE_LEADING_PAREN_END + _CORE_PREFIX_LOOP)],
    # JAN fuzzy_matcher.py:661-662.
    ("N05", "janitor-split"): [(_CORE_CAMEL_END,
                                _CORE_CAMEL_END
                                + "    name = re.sub(r'\\s*&\\s*', ' and ', name)\n")],
    # JAN fuzzy_matcher.py:637-642.
    ("N06", "janitor-letters"): [(_CORE_DOTS,
                                  "    name = re.sub(r'(?<=[A-Za-z])\\.(?=[A-Za-z])', ' ', name)\n")],
    # JAN fuzzy_matcher.py:729-735.
    ("N07", "janitor-floor"): [(_CORE_SUFFIX,
                                "    for _sfx in (r'\\s+Network\\s*$', r'\\s+Channel\\s*$', r'\\s+TV\\s*$'):\n"
                                "        _stripped = re.sub(_sfx, '', name, flags=re.IGNORECASE).strip()\n"
                                "        if _stripped and len(_stripped.split()) >= 2:\n"
                                "            name = _stripped\n")],
    # JAN/CM normalize_name have no Cf strip.
    ("N13", "keep"): [(_CORE_CF, "")],
    # JAN: bare/parenthesized East/West via its REGIONAL_EAST_WEST_PATTERNS
    # (swapped in as a table below) instead of the core's inline strip.
    ("N02", "janitor"): [(_CORE_EW, "")],
    # CM fuzzy_matcher.py:836-843: East/West kept, (E)/(W) promoted to words.
    ("N02", "cm-keep"): [(_CORE_EW,
                          "    name = re.sub(r'\\(\\s*(?:east|e)\\s*\\)', ' East ', name, flags=re.IGNORECASE)\n"
                          "    name = re.sub(r'\\(\\s*(?:west|w)\\s*\\)', ' West ', name, flags=re.IGNORECASE)\n")],
}

_LU_TRAILING = "if query_trailing_num is not None:"
_LU_SHIFT = "if query_is_shift != cand_is_shift:"
_LU_REGION = 'original_lower = (lineup_name or "").lower()'

#: match_all_streams statement patches applied to every ablation engine: the
#: shift check and the region filter move into post-filters (T07, ZONE) so
#: each variant is applied the same way; the trailing-number guard is T06.
BASE_MATCH_PATCHES = [
    (_LU_SHIFT, "if False:"),
    # A regionless query makes the region filter a no-op for one candidate
    # (the regionless branch keeps the set when it would empty it).
    (_LU_REGION, 'original_lower = ""'),
]


def _table_swaps(config: dict, mods) -> dict:
    """Module-global table values the configuration needs in the core."""
    core, jan, cm = mods["core"], mods["janitor"], mods["cm"]
    swaps = {}
    n01 = config.get("N01", "core-strip")
    if n01 == "janitor":
        swaps["QUALITY_PATTERNS"] = list(jan.QUALITY_PATTERNS)
    elif n01 == "cm-keep":
        swaps["QUALITY_PATTERNS"] = list(cm.QUALITY_PATTERNS)
    n04 = config.get("N04", "core")
    if n04 == "janitor":
        swaps["PROVIDER_PREFIX_PATTERNS"] = list(jan.PROVIDER_PREFIX_PATTERNS)
    elif n04 == "cm-strict":
        swaps["PROVIDER_PREFIX_PATTERNS"] = list(cm.PROVIDER_PREFIX_PATTERNS)
    n14 = config.get("N14", "core")
    if n14 == "janitor-anywhere":
        swaps["GEOGRAPHIC_PATTERNS"] = list(jan.GEOGRAPHIC_PATTERNS)
    elif n14 == "cm-double-tag":
        swaps["GEOGRAPHIC_PATTERNS"] = list(cm.GEOGRAPHIC_PATTERNS)
    n03 = config.get("N03", "core")
    if n03 == "janitor":
        swaps["MISC_PATTERNS"] = list(jan.MISC_PATTERNS)
    if config.get("N09", "strip") == "keep":
        swaps["_DECORATOR_CATS"] = frozenset()
    if config.get("N02", "core") == "janitor":
        swaps["REGIONAL_PATTERNS"] = (list(jan.REGIONAL_PATTERNS)
                                      + list(jan.REGIONAL_EAST_WEST_PATTERNS))
    for key in swaps:
        if not hasattr(core, key):
            raise RuntimeError(f"core has no table {key!r} to swap")
    return swaps


class _Swapped:
    """Install table swaps in the core module for one engine call, then restore."""

    def __init__(self, module, swaps):
        self.module = module
        self.swaps = swaps

    def __enter__(self):
        self.saved = {key: getattr(self.module, key) for key in self.swaps}
        for key, value in self.swaps.items():
            setattr(self.module, key, value)

    def __exit__(self, *exc):
        for key, value in self.saved.items():
            setattr(self.module, key, value)
        return False


# ---------------------------------------------------------------------------
# post-filters (written from the cited plugin source)
# ---------------------------------------------------------------------------

_PLUS_SHIFT_RE = re.compile(r'\+\s{0,2}\d{1,2}\b')  # JAN :99, LU :46 (identical)


def _zone_lu_like(query: str, candidate: str, pacific_or_west: bool,
                  regionless_drops: bool) -> bool:
    """JAN :1393-1466 / LU :951-1027 region filter for one candidate.

    ``regionless_drops`` applies the regionless branch as if a regionless
    sibling exists (the family view); the escape hatch that keeps a lone West
    candidate is a family-level fallback step 6 settles."""
    q = query.lower()
    c = candidate.lower()
    q_east = "east" in q or bool(re.search(r'\(\s*e\s*\)', q))
    q_west = ("west" in q and "western" not in q) or bool(re.search(r'\(\s*w\s*\)', q))
    q_pac = "pacific" in q or bool(re.search(r'\(\s*p\s*\)', q))
    s_east = "east" in c
    s_west = "west" in c and "western" not in c
    s_pac = "pacific" in c
    s_region = s_east or s_west or s_pac
    if q_east or q_west or q_pac:
        if q_east:
            if s_west and not s_east:
                return False
            if s_pac and not s_east:
                return False
        elif q_west:
            if s_east and not s_west and not s_pac:
                return False
            if not s_region:
                return False
        else:
            if pacific_or_west:
                if s_east and not s_pac and not s_west:
                    return False
            else:
                if s_east and not s_pac:
                    return False
                if s_west and not s_pac:
                    return False
            if not s_region:
                return False
        return True
    if regionless_drops and (s_pac or s_west):
        return False
    return True


def _zone_filter(variant: str, mods):
    sm_zone = mods["sm"].FuzzyMatcher.extract_zone
    sm = mods["sm"]

    def sm_zone_noguard(name):
        # extract_zone (SM :524-543) without the single-time-zone guard (Z02).
        name = name or ""
        if sm._ZONE_WEST_RE.search(name) or sm._ZONE_PACIFIC_RE.search(name):
            return "WEST"
        if sm._ZONE_EAST_RE.search(name):
            return "EAST"
        return "DEFAULT"

    def keep(query: str, candidate: str) -> bool:
        if variant == "none":
            return True
        if variant == "janitor":
            # Janitor's run path passes "regional" as an ignore tag, which
            # switches the regionless branch off (plugin.py:1127-1135).
            return _zone_lu_like(query, candidate, True, False)
        if variant == "lu":
            return _zone_lu_like(query, candidate, False, True)
        if variant == "sm-zone":
            # SM _order_streams_for_zone (plugin.py:4282-4330) for a routed
            # channel: a WEST channel keeps only WEST streams, any other keeps
            # only non-WEST ones. Modelled as routed: an unmarked channel
            # always is (`_zone_routed_map`, :4233-4264), and a marked one is
            # whenever its opposite-zone sibling exists, as it does in every
            # lineup carrying both feeds; the "keep the other zone when
            # nothing is left" fallback (issue 55) is the family-level escape
            # LU's regionless branch also has.
            return (sm_zone(query) == "WEST") == (sm_zone(candidate) == "WEST")
        if variant == "sm-zone-noguard":
            return ((sm_zone_noguard(query) == "WEST")
                    == (sm_zone_noguard(candidate) == "WEST"))
        raise ValueError(variant)
    return keep


def _digit_filter(variant: str, mods):
    """T07 variants on the engine's normalized names."""
    is_designator = mods["sm"].is_designator_token

    def keep(query: str, candidate: str, norm_q: str, norm_c: str) -> bool:
        shift_ok = (bool(_PLUS_SHIFT_RE.search(query))
                    == bool(_PLUS_SHIFT_RE.search(candidate)))
        q_digits = {t for t in norm_q.split() if t.isdigit()}
        c_digits = {t for t in norm_c.split() if t.isdigit()}
        disagree = bool(q_digits and c_digits and not (q_digits & c_digits))
        if variant == "off":          # candidate generation only
            return True
        if variant == "lu-none":      # LU :806-824: +1 shift equality only
            return shift_ok
        if variant == "janitor":      # JAN :1297-1315: disagreement + shift
            return shift_ok and not disagree
        if variant == "cm":           # CM match_all_streams: disagreement only
            return not disagree
        if variant == "janitor-designator":
            # Not a plugin variant: JAN's disagreement shape (numbered vs
            # numbered sharing nothing; never numbered vs unnumbered) over
            # SM's designator tokens, so `f1` vs `1` disagrees (SM issue #50)
            # while `ABC 7 (KGO)` vs `ABC (KGO)` still passes.
            q_des = {t for t in norm_q.split() if is_designator(t)}
            c_des = {t for t in norm_c.split() if is_designator(t)}
            return shift_ok and not (q_des and c_des and not (q_des & c_des))
        if variant == "sm-designator":  # SM :815-830 (asymmetric)
            q_des = {t for t in norm_q.split() if is_designator(t)}
            if not q_des:
                return True
            c_des = {t for t in norm_c.split() if is_designator(t)}
            return bool(c_des and (q_des & c_des))
        raise ValueError(variant)
    return keep


# ---------------------------------------------------------------------------
# the engine
# ---------------------------------------------------------------------------

#: Default (base) variant per ablated row: what Lineuparr's own engine does.
BASE = {
    "N01": "core-strip", "N02": "core", "N03": "core", "N04": "core",
    "N05": "keep", "N06": "core-wordchar", "N07": "unconditional",
    "N08": "split", "N09": "strip", "N11": "keep", "N12": "first",
    "N13": "strip", "N14": "core", "T01": "core", "T06": "jan-lu",
    "T07": "lu-none", "ZONE": "lu", "CALLSIGN": "lu", "A01": "lu",
    "A04": "lu-rescue",
}


def _merged_alias_map() -> dict:
    """The merged union (A03), as ``{canonical: [variant, ...]}``."""
    with open(os.path.join(MERGED, "aliases.json"), encoding="utf-8") as handle:
        data = json.load(handle)
    return {canonical: [entry["alias"] for entry in entries]
            for canonical, entries in sorted(data["aliases"].items())}


_KNOWN_SETS: dict = {}


def _known_set(source: str):
    """Known-callsign allowlists for the core ladder, from each plugin's data."""
    if source in _KNOWN_SETS:
        return _KNOWN_SETS[source]
    if source == "sm":
        known = oracles.get("sm").matcher._known_callsigns
    elif source == "janitor":
        known = oracles.get("janitor").matcher._get_known_callsigns()
    elif source == "merged":
        with open(os.path.join(MERGED, "stations.json"), encoding="utf-8") as handle:
            stations = json.load(handle)["stations"]
        known = set()
        for callsign in stations:
            known.add(callsign.upper())
            known.add(callsign.upper().split("-")[0])
    else:
        raise ValueError(source)
    _KNOWN_SETS[source] = frozenset(known or ())
    return _KNOWN_SETS[source]


#: CALLSIGN configurations: (ladder, known set, OTA-number rescue).
CALLSIGN_CONFIGS = {
    "lu": ("core", None, True),
    "core-sm": ("core", "sm", True),
    "core-sm-norescue": ("core", "sm", False),
    "core-merged": ("core", "merged", True),
    "core-merged-norescue": ("core", "merged", False),
    "janitor": ("janitor", None, False),
    "cm": ("cm", None, False),
}

#: How a CALLSIGN configuration reads as inventory rows C01-C04.
CALLSIGN_ROWS = {
    "lu": {"C01": "core-naked", "C02": "lu-none", "C03": "lu-none", "C04": "none"},
    "core-sm": {"C01": "core-known", "C02": "sm", "C03": "sm", "C04": "sm-rescue"},
    "core-sm-norescue": {"C01": "core-known", "C02": "sm", "C03": "sm", "C04": "none"},
    "core-merged": {"C01": "core-known", "C02": "merged", "C03": "sm",
                    "C04": "sm-rescue"},
    "core-merged-norescue": {"C01": "core-known", "C02": "merged", "C03": "sm",
                             "C04": "none"},
    "janitor": {"C01": "janitor", "C02": "janitor", "C03": "janitor", "C04": "none"},
    "cm": {"C01": "cm-lookup", "C02": "cm", "C03": "cm", "C04": "none"},
}

#: How a ZONE configuration reads as inventory rows Z01/Z04.
ZONE_ROWS = {
    "none": {"Z01": "none", "Z02": "none", "Z04": "none", "D09": "cm"},
    "sm-zone": {"Z01": "sm-zone", "Z02": "sm-guard", "Z04": "none", "D09": "sm-both"},
    "sm-zone-noguard": {"Z01": "sm-zone", "Z02": "none", "Z04": "none",
                        "D09": "sm-both"},
    "janitor": {"Z01": "none", "Z02": "none", "Z04": "janitor", "D09": "janitor"},
    "lu": {"Z01": "none", "Z02": "none", "Z04": "lu", "D09": "lu"},
}


class Engine:
    """The base engine with one configuration of the ablated rows applied."""

    def __init__(self, config: dict, threshold: int, alias_map: dict):
        mods = _mods()
        self.mods = mods
        self.config = {**BASE, **config}
        cfg = self.config
        core, lu = mods["core"], mods["lu"]
        self.swaps = _table_swaps(cfg, mods)

        norm_patches = []
        for (row, variant), patches in sorted(NORMALIZE_PATCHES.items()):
            if cfg.get(row) == variant:
                norm_patches.extend(patches)
        normalize = _patched_function(core.FuzzyMatcherCore.normalize_name,
                                      norm_patches, core)
        match_patches = list(BASE_MATCH_PATCHES)
        if cfg["T06"] == "sm":
            match_patches.append((_LU_TRAILING, "if False:"))
        match_all = _patched_function(lu.FuzzyMatcher.match_all_streams,
                                      match_patches, lu)

        attrs = {"_STRIP_BARE_REGION": cfg["N02"] == "lu-strip",
                 "match_all_streams": match_all}
        rejoin = cfg["N08"] == "sm-rejoin"
        cinemax = cfg["N11"] == "sm-strip"
        sm = mods["sm"]

        memo: dict = {}

        def normalize_name(inner, name, *args, **kwargs):
            # SM fuzzy_match strips Cinemax from CANDIDATES only, when the
            # channel name contains "max" (fuzzy_matcher.py:778-800).
            if cinemax and inner._cinemax_pair and name != inner._query:
                kwargs["remove_cinemax"] = True
            # normalize_name is pure for a fixed configuration: memoize it.
            key = (name, repr(args), repr(sorted(kwargs.items())))
            if key in memo:
                return memo[key]
            memo[key] = out = _normalize_repaired(inner, name, args, kwargs)
            return out

        def _normalize_repaired(inner, name, args, kwargs):
            out = normalize(inner, name, *args, **kwargs)
            if not rejoin or not out:
                return out
            # SM fuzzy_matcher.py:161-193, the designator repair.
            glued = {m.group(0).lower()
                     for m in sm._SOURCE_DESIGNATOR_RE.finditer(name or "")}
            if not glued:
                return out

            def _repair(match):
                joined = match.group(1) + match.group(2)
                return joined if joined.lower() in glued else match.group(0)
            return sm._REJOIN_DESIGNATOR_RE.sub(_repair, out)

        attrs["normalize_name"] = normalize_name
        t01 = cfg["T01"]
        if t01 == "janitor":
            attrs["_has_token_overlap"] = staticmethod(
                mods["janitor"].FuzzyMatcher._has_token_overlap)
        elif t01 == "cm":
            attrs["_has_token_overlap"] = staticmethod(
                mods["cm"].FuzzyMatcher._has_token_overlap)
        elif t01 == "sm-never":
            attrs["_has_token_overlap"] = staticmethod(lambda *a, **k: True)
        ladder, known, rescue = CALLSIGN_CONFIGS[cfg["CALLSIGN"]]
        if cfg["A04"] == "none":
            # LU fuzzy_matcher.py:595-620 off: no alias-declared callsign rescue.
            attrs["_alias_callsign_matches"] = lambda self, *a, **k: []
        if not rescue:
            attrs["_OTA_NUMBER_CONTEXT"] = re.compile(r"(?!x)x")
        cls = type("AblationMatcher", (lu.FuzzyMatcher,), attrs)
        matcher = cls(match_threshold=threshold)
        matcher._cinemax_pair = False
        matcher._query = None
        if ladder == "core" and known:
            matcher.set_known_callsigns(_known_set(known))
        elif ladder in ("janitor", "cm"):
            matcher._extract_callsign_with_confidence = (
                oracles.get(ladder).matcher._extract_callsign_with_confidence)
        self._install_alias(matcher, cfg["A01"], alias_map)
        self.matcher = matcher
        self.alias_map = alias_map
        self.zone = _zone_filter(cfg["ZONE"], mods)
        self.digits = _digit_filter(cfg["T07"], mods)

    def _install_alias(self, matcher, variant: str, alias_map: dict) -> None:
        mods = self.mods
        if variant == "lu":
            return
        if variant == "janitor":
            matcher.alias_match = types.MethodType(
                mods["janitor"].FuzzyMatcher.alias_match, matcher)
            return
        if variant == "sm":
            sm_cls = mods["sm"].FuzzyMatcher
            matcher._fold_alias_key = sm_cls._fold_alias_key
            matcher._folded_alias_index = types.MethodType(
                sm_cls._folded_alias_index, matcher)
            lookup = types.MethodType(sm_cls.alias_lookup, matcher)

            def sm_alias(lineup_name, candidate_names, amap, user_ignored_tags=None):
                return [(name, 100, "alias")
                        for name in lookup(lineup_name, candidate_names, amap,
                                           user_ignored_tags)]
            matcher.alias_match = sm_alias
            return
        if variant == "cm":
            cm_cls = mods["cm"].FuzzyMatcher
            matcher.alias_map = alias_map
            types.MethodType(cm_cls._rebuild_reverse_alias_index, matcher)()
            cm_match = types.MethodType(cm_cls.alias_match, matcher)

            def cm_alias(lineup_name, candidate_names, amap, user_ignored_tags=None):
                name, score, mtype = cm_match(lineup_name, candidate_names,
                                              user_ignored_tags)
                return [(name, score, mtype)] if name else []
            matcher.alias_match = cm_alias
            return
        if variant == "identity":
            # Not a plugin variant: the plan's identity resolve (step 6) applied
            # to aliases. Both names resolve through CM's reverse index
            # (canonical and every variant, first-wins) and match when they
            # resolve to the same canonical; LU's lookup still runs first.
            cm_cls = mods["cm"].FuzzyMatcher
            matcher.alias_map = alias_map
            types.MethodType(cm_cls._rebuild_reverse_alias_index, matcher)()
            index = matcher._reverse_alias_index
            lu_alias = matcher.alias_match

            def resolve(name, tags):
                norm = (matcher.normalize_name(name, tags) or "").lower()
                if not norm:
                    return None
                return index.get(norm) or index.get(re.sub(r"[\s&\-]+", "", norm))

            def identity_alias(lineup_name, candidate_names, amap, user_ignored_tags=None):
                hits = lu_alias(lineup_name, candidate_names, amap, user_ignored_tags)
                found = {name for name, _score, _type in hits}
                query_id = resolve(lineup_name, user_ignored_tags)
                if query_id:
                    for candidate in candidate_names:
                        if candidate not in found and resolve(
                                candidate, user_ignored_tags) == query_id:
                            hits.append((candidate, 100, "alias"))
                return hits
            matcher.alias_match = identity_alias
            return
        raise ValueError(variant)

    def normalize(self, name: str) -> str:
        with _Swapped(self.mods["core"], self.swaps):
            return self.matcher.normalize_name(name, []) or ""

    def same(self, query: str, candidate: str) -> bool:
        matcher = self.matcher
        matcher._cinemax_pair = "max" in (query or "").lower()
        matcher._query = query
        with _Swapped(self.mods["core"], self.swaps):
            ranked = matcher.match_all_streams(query, [candidate], self.alias_map)
            if not ranked:
                return False
            _name, _score, method = ranked[0]
            if method == "callsign":
                # Callsign-anchored candidates skip the region filter and the
                # fuzzy-loop guards (JAN :1373-1391, LU :871-893, :970).
                return True
            norm_q = (matcher.normalize_name(query, []) or "").lower()
            norm_c = (matcher.normalize_name(candidate, []) or "").lower()
        # Alias hits are added before the fuzzy loop, so the digit and shift
        # guards never see them; the region filter runs over every match.
        if method not in ("alias", "alias-callsign") and not self.digits(
                query, candidate, norm_q, norm_c):
            return False
        return self.zone(query, candidate)


# ---------------------------------------------------------------------------
# groups and coordinate descent
# ---------------------------------------------------------------------------

#: Joint search groups: row -> variants. Rows in one group are searched over
#: the full product with the other groups fixed.
GROUPS: list[tuple[str, dict[str, list[str]]]] = [
    ("quality", {"N01": ["core-strip", "janitor", "cm-keep"]}),
    ("prefix", {"N04": ["core", "janitor", "cm-strict"],
                "N12": ["first", "after-hyphen"],
                "N14": ["core", "janitor-anywhere", "cm-double-tag"]}),
    ("zone", {"N02": ["core", "janitor", "cm-keep", "lu-strip"],
              "ZONE": ["lu", "janitor", "sm-zone", "sm-zone-noguard", "none"]}),
    ("misc", {"N03": ["core", "janitor"]}),
    ("amp", {"N05": ["keep", "janitor-split"]}),
    ("dots", {"N06": ["core-wordchar", "janitor-letters"]}),
    ("suffix", {"N07": ["unconditional", "janitor-floor"]}),
    ("unicode", {"N09": ["strip", "keep"], "N13": ["strip", "keep"]}),
    ("cinemax", {"N11": ["keep", "sm-strip"]}),
    ("overlap", {"T01": ["core", "janitor", "cm", "sm-never"]}),
    ("digits", {"T07": ["lu-none", "janitor", "cm", "sm-designator",
                        "janitor-designator"],
                "N08": ["split", "sm-rejoin"],
                "T06": ["jan-lu", "sm"]}),
    ("callsign", {"CALLSIGN": list(CALLSIGN_CONFIGS)}),
    ("alias", {"A01": ["lu", "janitor", "sm", "cm", "identity"],
               "A04": ["lu-rescue", "none"]}),
]

#: Variant preference per ablated row, strictest (fewest conflations) first:
#: the tie-break on equal cost, and the default when a row has no evidence.
PREFERENCE = {
    "N01": ["cm-keep", "janitor", "core-strip"],
    "N02": ["cm-keep", "core", "janitor", "lu-strip"],
    "N03": ["core", "janitor"],
    "N04": ["cm-strict", "janitor", "core"],
    "N05": ["keep", "janitor-split"],
    "N06": ["janitor-letters", "core-wordchar"],
    "N07": ["janitor-floor", "unconditional"],
    "N08": ["sm-rejoin", "split"],
    "N09": ["keep", "strip"],
    "N11": ["keep", "sm-strip"],
    "N12": ["after-hyphen", "first"],
    "N13": ["keep", "strip"],
    "N14": ["core", "cm-double-tag", "janitor-anywhere"],
    "T01": ["core", "janitor", "cm", "sm-never"],
    "T06": ["jan-lu", "sm"],
    "T07": ["sm-designator", "janitor-designator", "janitor", "cm", "lu-none"],
    "ZONE": ["lu", "sm-zone-noguard", "sm-zone", "janitor", "none"],
    # No known set and no rescue first; then the merged station table (the
    # plan's one station table) before a single plugin's.
    "CALLSIGN": ["lu", "core-merged-norescue", "core-sm-norescue", "core-merged",
                 "core-sm", "janitor", "cm"],
    "A01": ["sm", "cm", "lu", "janitor", "identity"],
    "A04": ["none", "lu-rescue"],
}
STRICTEST = {row: variants[0] for row, variants in PREFERENCE.items()}


def preference_for(row: str, config: dict) -> list[str]:
    """PREFERENCE, made coherent with the rest of the configuration: a row
    with no evidence takes the variant its decided partner was written for.

    * N12: the core's prefix table expects the hyphen still in place
      (`^(?:US|...)\\s*[:\\-|]\\s*`; `matching_core.py:349-353` says why it
      strips first), so with N04 = core the core order comes first.
    * N08: designator repair only matters to a designator guard (SM's own
      docstring: rejoining RAISES `Sky Sports F1 UHD` vs `Sky Sports UHD 1`;
      the guard refuses it), so without one the split JAN/CM/LU pair with
      their isdigit guards comes first."""
    order = list(PREFERENCE[row])
    if row == "N12" and config.get("N04") == "core":
        order = ["first", "after-hyphen"]
    if row == "N08" and config.get("T07") not in ("sm-designator", "janitor-designator"):
        order = ["split", "sm-rejoin"]
    return order

#: Fewer discriminating pairs than this and a row is `no-evidence` (its
#: strictest variant) unless the few pairs favour another by more than NOISE,
#: which makes it `weak`.
MIN_EVIDENCE = 10

#: Costs within this of the cheapest are a tie (one wrong pair either way is
#: noise on a write path); the tie goes to the stricter variant.
NOISE = 2

#: Region/zone/shift markers: pairs without any are unaffected by the zone group.
_REGION_RE = re.compile(
    r"east|west|pacific|central|mountain|atlantic|\(\s*[ewp]\s*\)", re.IGNORECASE)
_DIGIT_RE = re.compile(r"\d|\bone\b|\btwo\b|\bthree\b|\bfour\b|\bfive\b", re.IGNORECASE)


def _cost(verdicts: list[bool], labels: list[str]) -> dict:
    tp = sum(1 for v, lab in zip(verdicts, labels) if v and lab == "same")
    fp = sum(1 for v, lab in zip(verdicts, labels) if v and lab == "different")
    fn = sum(1 for v, lab in zip(verdicts, labels) if not v and lab == "same")
    tn = sum(1 for v, lab in zip(verdicts, labels) if not v and lab == "different")
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "cost": FP_WEIGHT * fp + fn}


class Ablation:
    """Coordinate descent over GROUPS on construction-labelled pairs."""

    def __init__(self, pairs: list[dict]):
        # AI labels (a ``--ai`` rerun) are independent of every plugin, so
        # they count; oracle-voted labels do not (they carry the base's vote).
        self.pairs = [pair for pair in pairs
                      if pair["label_source"] in ("construction", "ai")
                      and pair["label"] in ("same", "different")]
        self.alias_map = _merged_alias_map()
        self._engines: dict = {}
        self._verdicts: dict = {}
        self._norm_names = sorted({name for pair in self.pairs
                                   for name in (pair["query"], pair["candidate"])})

    def engine(self, config: dict, threshold: int) -> Engine:
        key = (tuple(sorted({**BASE, **config}.items())), threshold)
        if key not in self._engines:
            self._engines[key] = Engine(config, threshold, self.alias_map)
        return self._engines[key]

    def _accepted(self, config: dict, indexes: list[int]) -> set[int]:
        """Pairs a configuration accepts (at either threshold)."""
        out = set()
        for threshold in THRESHOLDS:
            for index, verdict in zip(indexes, self.verdicts(config, threshold, indexes)):
                if verdict:
                    out.add(index)
        return out

    def _accepted_at(self, config: dict, threshold: int, indexes: list[int]) -> set[int]:
        return {index for index, verdict in
                zip(indexes, self.verdicts(config, threshold, indexes)) if verdict}

    def _candidate_pairs(self, group: str, rows: dict, config: dict) -> list[int]:
        """Pair indexes the group's variants can possibly disagree on.

        Guards and filters only ever REJECT, so for them the candidates are the
        pairs the guard-free engine accepts; normalizer rows can only move a
        pair whose name some variant normalizes differently; the callsign and
        alias groups only reach pairs where some ladder extracts a callsign on
        both sides, or where the query is an alias key."""
        everything = list(range(len(self.pairs)))
        if group == "zone":
            region = [i for i in everything
                      if _REGION_RE.search(self.pairs[i]["query"])
                      or _REGION_RE.search(self.pairs[i]["candidate"])]
            accepted: set[int] = set()
            for variant in rows["N02"]:
                accepted |= self._accepted({**config, "N02": variant, "ZONE": "none"},
                                           region)
            return sorted(accepted)
        if group == "digits":
            digit = [i for i in everything
                     if _DIGIT_RE.search(self.pairs[i]["query"])
                     or _DIGIT_RE.search(self.pairs[i]["candidate"])]
            accepted = set()
            for variant in rows["N08"]:
                accepted |= self._accepted({**config, "N08": variant, "T07": "off",
                                            "T06": "sm"}, digit)
            return sorted(accepted)
        if group == "overlap":
            return sorted(self._accepted({**config, "T01": "sm-never"}, everything))
        if group == "callsign":
            hits = set()
            for variant in rows["CALLSIGN"]:
                matcher = self.engine({**config, "CALLSIGN": variant}, THRESHOLDS[0]).matcher
                for name in self._norm_names:
                    callsign, high = matcher._extract_callsign_with_confidence(name)
                    if callsign and high:
                        hits.add(name)
            return [i for i in everything
                    if self.pairs[i]["query"] in hits and self.pairs[i]["candidate"] in hits]
        if group == "alias":
            keys = {key.strip().casefold() for key in self.alias_map}
            spellings = set()
            engine = self.engine(config, THRESHOLDS[0])
            for canonical, variants in self.alias_map.items():
                for spelling in [canonical] + list(variants):
                    spellings.add(re.sub(r"[\s&\-]+", "", engine.normalize(spelling).lower()))
            spellings.discard("")

            def touches(name):
                return (name.strip().casefold() in keys
                        or re.sub(r"[\s&\-]+", "", engine.normalize(name).lower()) in spellings)
            return [i for i in everything
                    if touches(self.pairs[i]["query"]) or touches(self.pairs[i]["candidate"])]
        if group == "cinemax":
            return [i for i in everything if "max" in self.pairs[i]["query"].lower()]
        # Normalizer groups: a pair can only change when some variant changes
        # the normalized form of one of its names.
        changed: set[str] = set()
        configs = self._product(rows, config)
        engines = [self.engine(cfg, THRESHOLDS[0]) for cfg in configs]
        for name in self._norm_names:
            forms = {engine.normalize(name) for engine in engines}
            if len(forms) > 1:
                changed.add(name)
        return [i for i in everything
                if self.pairs[i]["query"] in changed or self.pairs[i]["candidate"] in changed]

    @staticmethod
    def _product(rows: dict, config: dict) -> list[dict]:
        configs = [dict(config)]
        for row, variants in rows.items():
            configs = [{**cfg, row: variant} for cfg in configs for variant in variants]
        return configs

    def verdicts(self, config: dict, threshold: int, indexes: list[int]) -> list[bool]:
        engine = self.engine(config, threshold)
        key = (tuple(sorted(engine.config.items())), threshold)
        cache = self._verdicts.setdefault(key, {})
        out = []
        for index in indexes:
            if index not in cache:
                pair = self.pairs[index]
                try:
                    cache[index] = bool(engine.same(pair["query"], pair["candidate"]))
                except Exception:  # noqa: BLE001 - a crashing variant matches nothing
                    cache[index] = False
            out.append(cache[index])
        return out

    def decide_group(self, group: str, rows: dict, config: dict) -> dict:
        self._config = dict(config)
        indexes = self._candidate_pairs(group, rows, config)
        configs = self._product(rows, config)
        per_threshold = {}
        star = None
        for threshold in THRESHOLDS:
            table = {}
            verdict_rows = {}
            # The full product at the first cut; at the second, only the
            # one-row-at-a-time neighbours of the winner (the stability check).
            for cfg in (configs if star is None else star):
                verdict_rows[_cfg_key(cfg, rows)] = self.verdicts(cfg, threshold, indexes)
            # Discriminating pairs: some configuration disagrees with another.
            disc = [pos for pos in range(len(indexes))
                    if len({verdict_rows[key][pos] for key in verdict_rows}) > 1]
            labels = [self.pairs[indexes[pos]]["label"] for pos in disc]
            for key, verdicts in verdict_rows.items():
                table[key] = _cost([verdicts[pos] for pos in disc], labels)
            per_threshold[threshold] = {"table": table, "discriminating": disc,
                                        "indexes": indexes, "verdicts": verdict_rows}
            if star is None:
                winner = self._settle(rows, self._winner(rows, per_threshold[threshold]),
                                      verdict_rows, indexes, table)
                star = [cfg for cfg in configs
                        if sum(1 for row in rows if cfg[row] != winner[rows_index(rows, row)])
                        <= 1]
        return {"winner": dict(zip(rows, winner)), "per_threshold": per_threshold,
                "rows": list(rows)}

    def _settle(self, rows: dict, winner: tuple, verdict_rows: dict,
                indexes: list[int], table: dict) -> tuple:
        """A row of the winning product with fewer than MIN_EVIDENCE
        discriminating pairs (the others held) takes its preferred variant:
        the group's cost may favour it by a pair or two, which is noise."""
        winner = list(winner)
        for _ in range(len(rows)):
            changed = False
            for pos, row in enumerate(rows):
                verdicts = [verdict_rows[tuple(winner[:pos] + [variant] + winner[pos + 1:])]
                            for variant in rows[row]]
                disc = sum(1 for i in range(len(indexes))
                           if len({v[i] for v in verdicts}) > 1)
                if disc < MIN_EVIDENCE:
                    cfg = {**self._config, **dict(zip(rows, winner))}
                    preferred = preference_for(row, cfg)[0]
                    swapped = tuple(winner[:pos] + [preferred] + winner[pos + 1:])
                    # Few pairs still outvote the preference when they point
                    # one way by more than noise.
                    if (winner[pos] != preferred and table[swapped]["cost"]
                            <= table[tuple(winner)]["cost"] + NOISE):
                        winner[pos] = preferred
                        changed = True
            if not changed:
                break
        return tuple(winner)

    def _winner(self, rows: dict, data: dict) -> tuple:
        """Lowest cost; ties (and no-evidence groups) go to the strictest."""
        table = data["table"]

        def preference(key):
            cfg = {**self._config, **dict(zip(rows, key))}
            return sum(preference_for(row, cfg).index(variant)
                       for row, variant in zip(rows, key))
        floor = min(cost["cost"] for cost in table.values())
        near = [key for key in table if table[key]["cost"] <= floor + NOISE]
        return min(near, key=lambda key: (preference(key), table[key]["cost"], key))

    def run(self, rounds: int = 6) -> dict:
        config = dict(BASE)
        results = {}
        for _round in range(rounds):
            changed = False
            for group, rows in GROUPS:
                result = self.decide_group(group, rows, config)
                results[group] = result
                for row, variant in result["winner"].items():
                    if config.get(row) != variant:
                        config[row] = variant
                        changed = True
            if not changed:
                # Every group was re-decided with the final configuration.
                return {"config": config, "groups": results, "rounds": _round + 1}
        raise RuntimeError("ablation coordinate descent did not converge")


def discriminating_count(disc: list) -> int:
    return len(disc)


def _pref(row: str, variant: str) -> int:
    return PREFERENCE[row].index(variant) if variant in PREFERENCE.get(row, []) else 99


#: Base-score cuts swept on the final configuration (rows T10/D02: one
#: `[matching]` threshold for every app).
SWEEP = (70, 75, 80, 85, 90, 95)


def threshold_sweep(ablation: "Ablation", config: dict) -> dict:
    """Cost of the final configuration at every cut, on all construction pairs.

    A higher cut can only reject a pair the matcher scored (alias hits and
    callsign anchors score 100/95 and pass anyway), so pairs rejected at the
    lowest cut are rejected everywhere and only the accepted ones are re-run."""
    everything = list(range(len(ablation.pairs)))
    accepted = sorted(ablation._accepted_at(config, SWEEP[0], everything))
    labels_all = [pair["label"] for pair in ablation.pairs]
    table = {}
    for threshold in SWEEP:
        verdicts = dict.fromkeys(everything, False)
        for index, verdict in zip(accepted,
                                  ablation.verdicts(config, threshold, accepted)):
            verdicts[index] = verdict
        table[threshold] = _cost([verdicts[i] for i in everything], labels_all)
    return table


def rows_index(rows: dict, row: str) -> int:
    return list(rows).index(row)


def _cfg_key(cfg: dict, rows: dict) -> tuple:
    return tuple(cfg[row] for row in rows)


def _compare(rows_v: dict, winner: str, indexes: list[int], pairs: list[dict]) -> dict:
    """Cost table, discriminating pairs and examples for one row's variants."""
    disc = [pos for pos in range(len(indexes))
            if len({rows_v[v][pos] for v in rows_v}) > 1]
    labels = [pairs[indexes[pos]]["label"] for pos in disc]
    table = {variant: _cost([rows_v[variant][pos] for pos in disc], labels)
             for variant in rows_v}
    examples = {}
    for variant in rows_v:
        if variant == winner:
            continue
        shown = []
        for pos in disc:
            if rows_v[winner][pos] != rows_v[variant][pos]:
                pair = pairs[indexes[pos]]
                right = (rows_v[winner][pos]) == (pair["label"] == "same")
                shown.append({"query": pair["query"], "candidate": pair["candidate"],
                              "label": pair["label"], "generator": pair["generator"],
                              "winner_right": right})
        # Deterministic: the first two each way, in pair order.
        examples[variant] = ([item for item in shown if item["winner_right"]][:2]
                             + [item for item in shown if not item["winner_right"]][:2])
    return {"table": table, "discriminating": len(disc),
            "deciding": [indexes[pos] for pos in disc], "examples": examples}


def _best(table: dict, disc: int, preference: list[str]) -> str:
    def pref(variant):
        return preference.index(variant) if variant in preference else 99
    floor = min(cost["cost"] for cost in table.values())
    near = [v for v in table if table[v]["cost"] <= floor + NOISE]
    return min(near, key=lambda v: (pref(v), table[v]["cost"], v))


def row_reports(run: dict, pairs: list[dict], derived: dict | None = None) -> dict:
    """Per-row evidence at the final configuration: one-at-a-time comparisons.

    For each row the other rows stay at the final winner, so the counts say
    what THIS row changes in the engine that will ship. ``derived`` maps an
    inventory row to (pseudo-row, pseudo-variant -> row variant): each row
    variant is represented by its cheapest pseudo-variant, and only the pairs
    where those representatives disagree count for the row."""
    config = run["config"]
    reports = {}
    parents: dict = {}
    for group, rows in GROUPS:
        result = run["groups"][group]
        for row in rows:
            winner = config[row]
            per = {}
            parent_rows_v = {}
            for threshold in THRESHOLDS:
                data = result["per_threshold"][threshold]
                others = {r: config[r] for r in rows if r != row}
                keys = {variant: tuple(others[r] if r != row else variant for r in rows)
                        for variant in rows[row]}
                rows_v = {variant: data["verdicts"][key] for variant, key in keys.items()}
                parent_rows_v[threshold] = (rows_v, data["indexes"])
                per[threshold] = _compare(rows_v, winner, data["indexes"], pairs)
            preference = preference_for(row, config)
            stable = (_best(per[THRESHOLDS[1]]["table"], per[THRESHOLDS[1]]["discriminating"],
                            preference) == winner)
            reports[row] = {"group": group, "variant": winner, "preference": preference,
                            "stable_across_thresholds": stable, "per_threshold": per}
            parents[row] = parent_rows_v
    for row, (parent, mapping) in sorted((derived or {}).items()):
        parent_winner = config[parent]
        winner = mapping[parent_winner]
        preference = list(dict.fromkeys(mapping[v] for v in preference_for(parent, config)))
        rows_v80, indexes80 = parents[parent][THRESHOLDS[0]]
        full = _compare(rows_v80, parent_winner, indexes80, pairs)["table"]
        reps = {}
        for pseudo in preference_for(parent, config):
            variant = mapping[pseudo]
            if variant == winner:
                reps[variant] = parent_winner
            elif variant not in reps or full[pseudo]["cost"] < full[reps[variant]]["cost"]:
                reps[variant] = pseudo
        per = {}
        for threshold in THRESHOLDS:
            rows_v, indexes = parents[parent][threshold]
            per[threshold] = _compare({variant: rows_v[pseudo] for variant, pseudo in reps.items()},
                                      winner, indexes, pairs)
        stable = (_best(per[THRESHOLDS[1]]["table"], per[THRESHOLDS[1]]["discriminating"],
                        preference) == winner)
        reports[row] = {"group": reports[parent]["group"], "variant": winner,
                        "preference": preference, "representatives": reps,
                        "stable_across_thresholds": stable, "per_threshold": per}
    return reports


def result_label(pairs: list[dict], index: int) -> str:
    return pairs[index]["label"]


def self_test() -> int:
    """Every anchor applies, and each variant moves the case it exists for."""
    alias_map = _merged_alias_map()
    failures = 0

    def norm(config, name):
        return Engine(config, 80, alias_map).normalize(name)

    def check(label, got, want):
        nonlocal failures
        ok = got == want
        failures += not ok
        print(f"  {'ok  ' if ok else 'FAIL'} {label}: {got!r} (want {want!r})")

    print("ablate.py self-test")
    check("N01 core strips HEVC", norm({}, "TNT HEVC"), "TNT")
    check("N01 janitor keeps HEVC", norm({"N01": "janitor"}, "TNT HEVC"), "TNT HEVC")
    check("N05 janitor &->and", norm({"N05": "janitor-split"}, "U&Yesterday"),
          "U and Yesterday")
    check("N06 letters keep 97.2", norm({"N06": "janitor-letters"}, "Radio 97.2"),
          "Radio 97.2")
    check("N07 floor keeps Golf Channel", norm({"N07": "janitor-floor"}, "Golf Channel"),
          "Golf Channel")
    check("N13 keep leaves ZWSP", "\u200b" in norm({"N13": "keep"}, "BB\u200bC One"), True)
    check("N12 after-hyphen keeps UK", norm({"N12": "after-hyphen", "N04": "janitor"},
                                          "UK - BBC One"),
          "UK BBC 1")
    check("N02 cm-keep keeps West", norm({"N02": "cm-keep"}, "HBO West"), "HBO West")
    check("N02 lu-strip strips Central", norm({"N02": "lu-strip"}, "Comedy Central"),
          "Comedy")
    check("N09 keep leaves star", "\u2605" in norm({"N09": "keep"}, "ESPN \u2605"), True)
    check("N08 rejoin keeps F1", norm({"N08": "sm-rejoin"}, "Sky Sports F1"),
          "Sky Sports F1")
    engine = Engine({}, 80, alias_map)
    check("base same HBO/HBO HD", engine.same("HBO", "HBO HD"), True)
    check("base +1 shift rejects", engine.same("Zorblax Movies", "Zorblax Movies +1"),
          False)
    check("zone lu rejects regionless vs West", engine.same("HBO", "HBO West"), False)
    check("zone none keeps it", Engine({"ZONE": "none"}, 80, alias_map).same(
        "HBO", "HBO West"), True)
    check("T07 cm accepts +1", Engine({"T07": "cm"}, 80, alias_map).same(
        "Zorblax Movies", "Zorblax Movies +1"), True)
    print("ok" if not failures else f"{failures} failure(s)")
    return 1 if failures else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
