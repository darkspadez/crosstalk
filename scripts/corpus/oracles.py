#!/usr/bin/env python3
"""Oracle runners: the four plugins' OWN decisions behind one small API.

Step 3 of plans/phase-3-lineup.md ("Rule inventory + oracle runners"). This module
changes no matching behaviour; it only loads each plugin's real code from
``reference/`` and exposes the same small surface over all four so step 5 can run
them over one corpus and keep where they disagree:

    normalize(name, flags) -> str
    callsign(name) -> (callsign | None, high_confidence: bool)
    alias(name, aliases) -> list of variants the plugin resolves for ``name``
    match(query, candidates, threshold) -> ranked [(candidate, score, method)]
    same(a, b, threshold) -> bool (would the plugin's run path pair a with b)

Each plugin is loaded in its own module namespace (``oracle_<tag>`` packages, one
per plugin directory) so their identically named modules (``matching_core``,
``aliases``, ``fuzzy_matcher``, ``plugin``) don't collide; the self-test in
``disagreements.py`` proves the isolation by module identity and by mutation.
Data each plugin ships (channel DBs, station tables, aliases) is loaded the way
the plugin itself loads it.

``match``/``same`` drive each plugin's real DECISION path, not just a matcher
entry point, because that is the behaviour the unified engine replaces:

* EPG Janitor (EPG assign): ``plugin.py:_find_best_epg_match`` (1076), imported
  for real with Dispatcharr stubbed. It scores every EPG entry as
  max(structural callsign/state/city/network score, ``match_all_streams`` score
  at the module constant ``FUZZY_MATCH_THRESHOLD`` = 85 with the plugin's own
  ``user_ignored_tags`` set), and the channel is assigned only when the winner's
  confidence reaches ``automatch_confidence_threshold`` / ``heal_confidence_threshold``
  (both default 95). ``threshold`` here is that assignment cut. The ranked list
  is the per-entry scores in the plugin's own stable sort; ``match(...)[0]`` is
  checked against the function's real winner.
* Stream-Mapparr (stream attach): ``plugin.py:_match_streams_to_channel`` (5808)
  then ``_streams_to_assign`` (4405) with the channel's own ``_zone_routed_map``
  (4233), imported for real with the stubs ``scripts/match_parity.py`` installs.
  That is the OTA callsign path (with common-word corroboration), the fuzzy
  winner as a gate, the alias force-include, the family collection against the
  CHANNEL name (exact / containment with number rule / token overlap), dedupe
  and the zone drop. The result is a set ordered by SM's quality sort; SM keeps
  no per-stream score, so ``score`` is the run's gate score (100 for callsign
  and alias) and ``method`` the plugin's reason string.
* Channel-Mapparr (channel naming): the premium half of
  ``load_and_process_channels_action`` (plugin.py:1975-2050), transcribed as
  control flow over CM's real matcher calls: a name from which
  ``match_broadcast_channel`` extracts ANY callsign takes the OTA path and is
  never matched to a premium name (so ``match`` is empty); otherwise
  ``build_token_index`` + ``get_candidates`` pre-filter, then ``fuzzy_match``
  picks the single rename target. CM's ``match_all_streams`` is not called by
  ``plugin.py`` at all, so it is not used here.
* Lineuparr (stream attach): ``match_all_streams`` exactly as ``plugin.py``
  calls it (2734, 3509): every returned stream is attached. The matcher runs at
  ``threshold`` (its own stage thresholds move with it, as with LU's presets).

``same(a, b, threshold)`` is "b is in ``match(a, [b], threshold)``".

Residual approximations (each oracle's docstring repeats its own):

* Janitor: every EPG entry is treated as having programme data
  (``allow_without_programs=True``); a real run skips entries without it.
* SM: one channel per group with default settings (``tag_handling=strip_all``,
  no ignore tags, country restriction off, ``visible_channel_limit`` and
  grouping not modelled), and the channel is assumed to hold no streams yet
  (issue 55's "leave a channel alone" never fires).
* CM: the OTA rename (formatted ``{NETWORK} - {STATE} {CITY} ({CALLSIGN})``
  name) is not produced; ``callsign_rename`` reports the station it resolves.
  CM's plugin breaks score ties by ``set`` iteration order (hash-seed
  dependent); the oracle breaks them by candidate order.
* LU: no lineup country (``lineup_country=None``), no channel-number boost, no
  quality-aware twins unless passed via ``context``.

Run with ``python3 -I`` (no pip installs). ``rapidfuzz`` is absent, so every
plugin uses the pure-Python Levenshtein fallback in
``FuzzyMatcherCore.calculate_similarity``; see inventory row R01 for where that
can differ from a production install that has rapidfuzz.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
import types

sys.dont_write_bytecode = True

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PLUGIN_DIRS = {
    # Short tag -> plugin directory holding fuzzy_matcher.py.
    "janitor": os.path.join(REPO, "reference", "Dispatcharr-EPG-Janitor-Plugin", "EPG-Janitor"),
    "sm": os.path.join(REPO, "reference", "Stream-Mapparr", "Stream-Mapparr"),
    "cm": os.path.join(REPO, "reference", "Dispatcharr-Channel-Maparr-Plugin", "Channel-Maparr"),
    "lu": os.path.join(REPO, "reference", "Dispatcharr-Lineuparr-Plugin", "Lineuparr"),
}

#: Human names for the four oracles, keyed by tag.
NAMES = {
    "janitor": "EPG Janitor",
    "sm": "Stream-Mapparr",
    "cm": "Channel-Mapparr",
    "lu": "Lineuparr",
}

#: The cut each plugin's run path decides at, by default.
#: Janitor: automatch/heal confidence threshold (plugin.py:220, :227), the fuzzy
#: pipeline inside always runs at FUZZY_MATCH_THRESHOLD = 85 (plugin.py:75).
#: SM: DEFAULT_FUZZY_MATCH_THRESHOLD = 85 (plugin.py:324). CM and LU: the
#: "normal" sensitivity preset, 80 (relaxed/normal/strict/exact = 70/80/90/95).
DEFAULT_THRESHOLDS = {"janitor": 95, "sm": 85, "cm": 80, "lu": 80}

#: CM's DEFAULT_IGNORED_TAGS (plugin.py:104), expanded the way
#: load_and_process_channels_action expands it (plugin.py:1925-1937).
_CM_DEFAULT_TAGS = "[4K], [FHD], [HD], [SD], [Unknown], [Unk], [Slow], [Dead]"


def _cm_ignored_tags():
    tags = []
    for tag in (t.strip() for t in _CM_DEFAULT_TAGS.split(",") if t.strip()):
        tags.append(tag)
        if tag.startswith("[") and tag.endswith("]"):
            tags.append(f"({tag[1:-1]})")
        elif tag.startswith("(") and tag.endswith(")"):
            tags.append(f"[{tag[1:-1]}]")
    return tags


#: The ``user_ignored_tags`` each plugin's run path passes to its matcher.
#: Janitor passes the four category WORDS (plugin.py:1127-1135); they are applied
#: as ignore tags by normalize_name and also switch off match_all_streams'
#: regionless West/Pacific filter ("regional" in user_ignored_tags, :1450).
RUN_IGNORED_TAGS = {
    "janitor": ["quality", "regional", "geographic", "misc"],
    "sm": [],
    "cm": _cm_ignored_tags(),
    "lu": [],
}

# Bare module names a plugin's fuzzy_matcher.py may import as top-level fallbacks
# (each file tries the relative import first, e.g. ``from .matching_core``).
_SHADOWED = ("matching_core", "aliases", "country", "market_index", "station_affiliation")

_QUIET_LOGGERS = (
    "plugins.epg_janitor.fuzzy_matcher", "plugins.epg_janitor",
    "plugins.fuzzy_matcher", "plugins.lineuparr.fuzzy_matcher",
    "plugins.lineuparr", "plugins.stream_mapparr", "matching_core",
)

class _StrictLogger:
    """The ``logger`` argument handed to plugin decision paths.

    Debug/info chatter is dropped. A warning or error is raised instead: both
    decision paths catch their own exceptions and log them (Janitor's
    ``_find_best_epg_match`` then returns no match, or falls back to difflib),
    which would otherwise turn a broken oracle into silent "no match" answers.
    """

    def debug(self, *args, **kwargs):
        pass

    info = debug

    def warning(self, message, *args, **kwargs):
        raise RuntimeError(f"plugin decision path logged a warning: {message}")

    def error(self, message, *args, **kwargs):
        raise RuntimeError(f"plugin decision path logged an error: {message}")

    exception = error


_LOG = _StrictLogger()


def _quiet():
    """The plugins log per-name chatter at INFO/DEBUG; keep WARNING. Log level only."""
    for logger_name in _QUIET_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.ERROR)


def _load_plugin(tag: str):
    """Import one plugin's ``fuzzy_matcher`` in an isolated namespace.

    Creates a synthetic package ``oracle_<tag>`` whose ``__path__`` is the plugin
    directory, then loads ``matching_core`` (plus ``aliases``/``country``/
    ``market_index``/``station_affiliation`` where present) and finally
    ``fuzzy_matcher`` as submodules of it, so relative imports
    (``from .matching_core import ...``, ``from . import country``) resolve to
    THIS plugin's files. The absolute fallbacks (``from matching_core import``)
    are satisfied by temporarily aliasing the bare names in ``sys.modules``
    during the exec only; they are restored afterwards, so sequential loads
    never leak one plugin's modules into another's.
    """
    plugindir = PLUGIN_DIRS[tag]
    pkg_name = f"oracle_{tag}"
    if pkg_name in sys.modules:
        return sys.modules[pkg_name + ".fuzzy_matcher"]

    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [plugindir]
    sys.modules[pkg_name] = pkg

    saved = {}
    try:
        for name in _SHADOWED:
            path = os.path.join(plugindir, name + ".py")
            if not os.path.exists(path):
                continue
            full = pkg_name + "." + name
            spec = importlib.util.spec_from_file_location(full, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[full] = module
            if name in sys.modules and name not in saved:
                saved[name] = sys.modules[name]
            sys.modules[name] = module
            spec.loader.exec_module(module)
            setattr(pkg, name, module)

        full = pkg_name + ".fuzzy_matcher"
        spec = importlib.util.spec_from_file_location(
            full, os.path.join(plugindir, "fuzzy_matcher.py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
        pkg.fuzzy_matcher = module
        _quiet()
        return module
    finally:
        for name in _SHADOWED:
            if os.path.exists(os.path.join(plugindir, name + ".py")):
                sys.modules.pop(name, None)
                if name in saved:
                    sys.modules[name] = saved[name]


# ---------------------------------------------------------------------------
# plugin.py loading (Janitor and Stream-Mapparr decision paths)
# ---------------------------------------------------------------------------

_STUBS_INSTALLED = False


def _install_dispatcharr_stubs():
    """Stand-ins for the Dispatcharr/Django modules ``plugin.py`` imports.

    Reuses ``scripts/match_parity.py:_install_runtime_stubs`` (the Stream-Mapparr
    test suite's own stubs) and adds the few names EPG Janitor's ``plugin.py``
    imports on top (its own test suite stubs them as MagicMocks,
    ``reference/Dispatcharr-EPG-Janitor-Plugin/tests/conftest.py:100``). The
    stubs answer imports only; no oracle path reaches the ORM.
    """
    global _STUBS_INSTALLED
    if _STUBS_INSTALLED:
        return
    from unittest.mock import MagicMock

    spec = importlib.util.spec_from_file_location(
        "corpus_match_parity_stubs", os.path.join(REPO, "scripts", "match_parity.py"))
    parity = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parity)
    parity._install_runtime_stubs()

    def _ensure(module_name, *attrs):
        module = sys.modules.get(module_name)
        if module is None:
            module = types.ModuleType(module_name)
            sys.modules[module_name] = module
        for attr in attrs:
            if not hasattr(module, attr):
                setattr(module, attr, MagicMock(name=f"{module_name}.{attr}"))
        return module

    _ensure("apps.epg.models", "EPGSource", "EPGData", "ProgramData")
    _ensure("core.utils", "log_system_event", "send_websocket_update")
    _ensure("django.db.models", "Max")
    celery = _ensure("celery")

    def shared_task(*args, **kwargs):
        # Both decorator forms; the decorated function is returned unchanged.
        if args and callable(args[0]) and not kwargs:
            return args[0]
        return lambda func: func

    celery.shared_task = shared_task
    _STUBS_INSTALLED = True


def _load_plugin_module(tag: str):
    """Import ``plugin.py`` as ``oracle_<tag>.plugin`` (after ``_load_plugin``)."""
    full = f"oracle_{tag}.plugin"
    if full in sys.modules:
        return sys.modules[full]
    _install_dispatcharr_stubs()
    spec = importlib.util.spec_from_file_location(
        full, os.path.join(PLUGIN_DIRS[tag], "plugin.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    sys.modules[f"oracle_{tag}"].plugin = module
    _quiet()
    return module


def _load_aliases(tag: str) -> dict:
    """Load a plugin's shipped alias table the way its own code reads it."""
    aliases_mod = sys.modules.get(f"oracle_{tag}.aliases")
    if aliases_mod is None:
        raise RuntimeError(f"plugin {tag!r} ships no aliases.py")
    return dict(getattr(aliases_mod, "CHANNEL_ALIASES", {}))


class _Threshold:
    """Run a matcher at ``threshold`` for one call, then restore it."""

    def __init__(self, matcher, threshold):
        self.matcher = matcher
        self.threshold = threshold

    def __enter__(self):
        self.saved = self.matcher.match_threshold
        self.matcher.match_threshold = self.threshold

    def __exit__(self, *exc):
        self.matcher.match_threshold = self.saved
        return False


class Oracle:
    """One plugin's own matcher and decision path behind the shared oracle API."""

    tag: str

    def __init__(self, tag: str):
        self.tag = tag
        self.fm_mod = _load_plugin(tag)
        self.plugin_dir = PLUGIN_DIRS[tag]
        self.matcher = self._build_matcher()
        self.ignored_tags = list(RUN_IGNORED_TAGS[tag])
        # The alias map each run path uses with default settings: Janitor's
        # _build_alias_map (CHANNEL_ALIASES, no custom JSON), SM's
        # _build_alias_map with no country, LU's alias_map_for(None) (base
        # table), CM's matcher-owned table (built-ins; set_user_aliases extends).
        self.alias_map = self._build_alias_map()
        self.plugin = None
        if tag in ("janitor", "sm"):
            self.plugin = self._build_plugin()

    # -- construction ------------------------------------------------------

    def _build_matcher(self):
        fm = self.fm_mod.FuzzyMatcher
        if self.tag == "janitor":
            # plugin.py:398 builds with FUZZY_MATCH_THRESHOLD (85); databases
            # are loaded on demand via reload_databases (None = all countries).
            matcher = fm(plugin_dir=self.plugin_dir, match_threshold=85)
            matcher.reload_databases()
            return matcher
        if self.tag == "sm":
            # Constructor loads all sixteen *_channels.json databases plus the
            # networks.json FCC station table when plugin_dir is given.
            return fm(plugin_dir=self.plugin_dir, match_threshold=85)
        if self.tag == "cm":
            # Constructor stays cheap (incident 2026-06-27, see its NOTE); the
            # run path loads on demand, so the oracle does the same. Default
            # threshold 80 = the "normal" sensitivity preset.
            matcher = fm(plugin_dir=self.plugin_dir, match_threshold=80)
            matcher.reload_databases()
            return matcher
        if self.tag == "lu":
            # No plugin_dir parameter and no channel database: Lineuparr matches
            # lineup entries against stream names with per-call alias maps.
            return fm(match_threshold=80)
        raise ValueError(f"unknown oracle tag {self.tag!r}")

    def _build_alias_map(self) -> dict:
        if self.tag == "cm":
            return dict(self.matcher.alias_map)
        return _load_aliases(self.tag)

    def _build_plugin(self):
        """A ``Plugin`` instance carrying only what the decision path reads."""
        module = _load_plugin_module(self.tag)
        cls = module.Plugin
        inst = cls.__new__(cls)
        inst.fuzzy_matcher = self.matcher
        if self.tag == "sm":
            # _ensure_matcher_and_aliases (4114) with default settings.
            inst._alias_map = cls._build_alias_map(inst, {}, None)
            self.alias_map = inst._alias_map
            # Sort state as scripts/match_parity.py primes it (defaults; no
            # probe/throughput data, so the throughput tier collapses).
            config = module.PluginConfig
            inst._throughput_state_primed = True
            inst.saved_settings = {}
            inst._prioritize_quality = bool(config.DEFAULT_PRIORITIZE_QUALITY)
            inst._throughput_sorting_enabled = True
            inst._throughput_cache = {}
            inst._bitrate_safety_margin = float(config.DEFAULT_BITRATE_SAFETY_MARGIN)
            inst._probe_cache_ttl_minutes = int(config.DEFAULT_PROBE_CACHE_TTL_MINUTES)
            inst._content_starved_enabled = cls._resolve_demote_content_starved({})
            inst._content_starved_floor_kbps = cls._resolve_content_starved_floor({})
            inst._bitrate_sort_step_kbps = cls._resolve_bitrate_sort_step({})
            # Residual: the channel holds no streams yet (issue 55 never fires).
            inst._channel_has_streams = lambda channel_id: False
        return inst

    # -- shared API --------------------------------------------------------

    def normalize(self, name, flags=None) -> str:
        """Run the plugin's own ``normalize_name``.

        ``flags`` selects the four ignore toggles (default True, as every
        plugin runs them), ``user_ignored_tags`` (default: what the plugin's run
        path passes, ``RUN_IGNORED_TAGS``) and ``remove_cinemax`` (default
        False). CM additionally honours ``remove_country_prefix``; LU/CM/core
        accept ``strip_bare_region`` (LU's class default is True).
        """
        flags = dict(flags or {})
        kwargs = {
            "user_ignored_tags": list(flags.get("user_ignored_tags", self.ignored_tags)),
            "ignore_quality": flags.get("ignore_quality", True),
            "ignore_regional": flags.get("ignore_regional", True),
            "ignore_geographic": flags.get("ignore_geographic", True),
            "ignore_misc": flags.get("ignore_misc", True),
        }
        if self.tag == "janitor":
            # Janitor resolves None through its instance toggles (all True), so
            # pass explicit booleans to pin the comparison.
            return self.matcher.normalize_name(name, **kwargs)
        if self.tag == "sm":
            return self.matcher.normalize_name(
                name, **kwargs, remove_cinemax=flags.get("remove_cinemax", False)
            )
        if self.tag == "cm":
            return self.matcher.normalize_name(
                name,
                **kwargs,
                remove_cinemax=flags.get("remove_cinemax", False),
                remove_country_prefix=flags.get("remove_country_prefix", False),
            )
        # Lineuparr uses the shared core's normalize_name as-is (plus the
        # class-level _STRIP_BARE_REGION opt-in).
        return self.matcher.normalize_name(name, **kwargs)

    def callsign(self, name):
        """Run the plugin's own callsign extraction.

        Returns ``(callsign | None, high_confidence)`` via the cached
        ``_extract_callsign_with_confidence`` wrapper every plugin inherits or
        overrides (parenthesized/end-of-name = high confidence, loose mid-name
        word = low).
        """
        return self.matcher._extract_callsign_with_confidence(name)

    def alias(self, name, aliases=None):
        """Return the alias variants the plugin resolves for ``name``.

        Mapping per plugin (each one's real lookup rule, without candidates):

        * Janitor: verbatim stripped-key lookup
          (``alias_map.get(name.strip())``, fuzzy_matcher.py:1082).
        * Stream-Mapparr: verbatim key first, then the case-folded fallback
          index (``_fold_alias_key``/``_folded_alias_index``, :737-742).
        * Channel-Mapparr: the canonical entry its normalized reverse index
          (``_reverse_alias_index``, spaced and nospace keys, first-wins on
          collision) maps ``name`` to, i.e. what ``alias_match`` fires on
          (:708-735). Uses the matcher's real index; ``aliases`` rebuilds it
          via ``set_user_aliases``.
        * Lineuparr: verbatim ``alias_map.get(name)`` (:460).
        """
        alias_map = dict(aliases) if aliases is not None else self.alias_map
        if self.tag == "janitor":
            return list(alias_map.get((name or "").strip(), []))
        if self.tag == "sm":
            variants = alias_map.get(name)
            if not variants:
                folded = self.matcher._folded_alias_index(alias_map)
                variants = folded.get(self.matcher._fold_alias_key(name), [])
            return list(variants or [])
        if self.tag == "cm":
            if aliases is not None:
                self.matcher.set_user_aliases(aliases)
            try:
                norm = self.matcher.normalize_name(name, self.ignored_tags)
                if not norm:
                    return []
                import re

                key = norm.lower()
                index = self.matcher._reverse_alias_index
                canonical = index.get(key) or index.get(re.sub(r"[\s&\-]+", "", key))
                if canonical is None:
                    return []
                return [canonical] + list(self.matcher.alias_map.get(canonical, []))
            finally:
                if aliases is not None:
                    self.matcher.set_user_aliases(None)
        # Lineuparr: verbatim lookup, no folding.
        return list(alias_map.get(name, []))

    def match(self, query, candidates, threshold=None, context=None):
        """The plugin's own decision for ``query`` over ``candidates``.

        Returns ``[(candidate, score, method)]``, best first (see the module
        docstring for each plugin's decision path and what ``score`` means).
        ``threshold`` defaults to the cut the plugin's run path decides at
        (``DEFAULT_THRESHOLDS``). ``context`` (LU only) may carry
        ``lineup_country``, ``channel_number``, ``quality_aware`` and
        ``candidate_countries``, passed to ``match_all_streams`` as-is.
        """
        threshold = DEFAULT_THRESHOLDS[self.tag] if threshold is None else threshold
        candidates = list(candidates or [])
        if not candidates:
            return []
        if self.tag == "janitor":
            return self._match_janitor(query, candidates, threshold)
        if self.tag == "sm":
            return self._match_sm(query, candidates, threshold)
        if self.tag == "cm":
            return self._match_cm(query, candidates, threshold)
        return self._match_lu(query, candidates, threshold, context or {})

    def same(self, a, b, threshold=None) -> bool:
        """Would the plugin's run path pair ``a`` (channel) with ``b``?"""
        return any(candidate == b for candidate, _, _ in self.match(a, [b], threshold))

    # -- per-plugin decision paths -------------------------------------------

    def _janitor_best(self, query, entries):
        return self.plugin._find_best_epg_match(
            query, entries, None, None, _LOG,
            allow_without_programs=True, alias_map=self.alias_map,
        )

    def _match_janitor(self, query, candidates, threshold):
        """EPG Janitor: ``_find_best_epg_match`` (plugin.py:1076) + assign cut.

        The function returns one winner; the ranked list is each entry scored
        on its own (every score in it is per entry: structural attributes,
        alias, fuzzy stages and the zoned region filter read one entry at a
        time, and the regionless filter is off on this run path), sorted the
        way the function sorts (stable, score desc). ``score`` is the plugin's
        confidence, ``min(score, 100)``; an entry is kept when it reaches the
        assignment ``threshold`` (automatch/heal default 95).
        Residual: every entry is assumed to carry programme data.
        """
        entries = [{"id": index, "name": name} for index, name in enumerate(candidates)]
        scored = []
        for entry in entries:
            epg, confidence, method = self._janitor_best(query, [entry])
            if epg is not None:
                scored.append((entry["name"], confidence, method))
        scored.sort(key=lambda item: item[1], reverse=True)
        if scored:
            winner, _, _ = self._janitor_best(query, entries)
            assert winner is not None and winner["name"] == scored[0][0], (
                f"janitor oracle ranking disagrees with _find_best_epg_match for {query!r}")
        return [item for item in scored if item[1] >= threshold]

    def _match_sm(self, query, candidates, threshold):
        """Stream-Mapparr: the collector plus the per-channel zone step.

        ``_match_streams_to_channel`` (plugin.py:5808) with the plugin's
        default flags, then ``_streams_to_assign`` (4405) with the channel's
        own ``_zone_routed_map`` (4233) for a one-channel group.
        """
        inst = self.plugin
        streams = [
            {"id": index, "name": name, "channel_group": None, "channel_group__name": None,
             "m3u_account": None, "url": None, "stats": {}, "is_custom": False, "tvg_id": ""}
            for index, name in enumerate(candidates)
        ]
        channel = {"id": 1, "name": query}
        with _Threshold(self.matcher, threshold):
            matched, _cleaned, _names, reason, _db = inst._match_streams_to_channel(
                channel, streams, _LOG, list(self.ignored_tags),
                True, True, True, True, channels_data=[],
            )
            zone_routed = inst._zone_routed_map(
                [channel], list(self.ignored_tags), True, True, True, True)
            assigned, keep_existing = inst._streams_to_assign(matched, channel["id"], zone_routed)
        if keep_existing or not assigned:
            return []
        score = 100
        if reason.startswith("Fuzzy match"):
            import re

            found = re.search(r"score: (\d+)\)", reason)
            score = int(found.group(1)) if found else 0
        return [(stream["name"], score, reason) for stream in assigned]

    def _match_cm(self, query, candidates, threshold):
        """Channel-Mapparr: the premium rename decision (plugin.py:1975-2050).

        A name with any extracted callsign takes the OTA path (station rename
        or skip) and never reaches premium matching, so it matches nothing
        here. Otherwise the token index over ``candidates`` pre-filters and
        ``fuzzy_match`` returns the single rename target.
        Residual: CM's ``get_candidates`` returns a ``set`` order, so equal-score
        ties are hash-seed dependent in the plugin; the oracle keeps input order.
        """
        callsign, _station = self.matcher.match_broadcast_channel(
            query, network=None, allow_market_fallback=False)
        if callsign:
            return []
        tags = list(self.ignored_tags)
        with _Threshold(self.matcher, threshold):
            self.matcher.build_token_index(candidates, tags)
            try:
                pre = self.matcher.get_candidates(query, tags)
            finally:
                self.matcher.build_token_index([], tags)
            if pre is None:
                pre = candidates
            members = set(pre)
            ordered = [name for name in candidates if name in members]
            if not ordered:
                return []
            name, score, mtype = self.matcher.fuzzy_match(query, ordered, tags)
        if name is None:
            return []
        return [(name, score, mtype)]

    def callsign_rename(self, name):
        """CM only: ``(callsign, station)`` the OTA rename path resolves, or ``(None, None)``."""
        if self.tag != "cm":
            raise ValueError("callsign_rename is Channel-Mapparr's OTA path")
        return self.matcher.match_broadcast_channel(name, network=None, allow_market_fallback=False)

    def _match_lu(self, query, candidates, threshold, context):
        """Lineuparr: ``match_all_streams`` as plugin.py calls it; all results attach."""
        with _Threshold(self.matcher, threshold):
            ranked = self.matcher.match_all_streams(
                query, candidates, self.alias_map,
                channel_number=context.get("channel_number"),
                lineup_country=context.get("lineup_country"),
                quality_aware=bool(context.get("quality_aware", False)),
                candidate_countries=context.get("candidate_countries"),
            )
        return [(name, score, mtype) for name, score, mtype in ranked]


_ORACLES: dict[str, Oracle] = {}


def get(tag: str) -> Oracle:
    """Load (once) and return the oracle for ``tag``."""
    if tag not in PLUGIN_DIRS:
        raise KeyError(f"unknown oracle {tag!r}; want one of {sorted(PLUGIN_DIRS)}")
    if tag not in _ORACLES:
        _ORACLES[tag] = Oracle(tag)
    return _ORACLES[tag]


def all_oracles() -> dict[str, Oracle]:
    """Load and return all four oracles keyed by tag."""
    return {tag: get(tag) for tag in PLUGIN_DIRS}
