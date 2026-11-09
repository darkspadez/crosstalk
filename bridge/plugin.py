"""crosstalk bridge: the four bulk writes Dispatcharr's REST API cannot do.

crosstalk reaches this through Dispatcharr's plugin-run API
(``POST /api/plugins/plugins/<key>/run/`` with ``{"action": op, "params": {...}}``), so every
request already carries a Dispatcharr admin credential. On top of that each request is signed:
``params`` is ``{"envelope": {...}, "mac": "<hex>"}`` and the MAC is HMAC-SHA256 under the
shared secret, over the canonical JSON of the envelope. The secret comes from Dispatcharr's
environment (``CROSSTALK_BRIDGE_SECRET``) when that is set, else from this plugin's settings. The
contract (operations, canonical JSON, limits, threat model) is ``docs/bridge.md`` in the
crosstalk repository; this file implements it and nothing else.

What it will do, all allow-listed and idempotent:

* ``ping``: answer the version, protocol and limits. Writes nothing.
* ``set_stream_lists``: replace channels' ordered stream lists (``ChannelStream`` rows).
* ``set_stream_stats``: set streams' ``stream_stats`` and ``stream_stats_updated_at``.
* ``clear_channel_groups``: set channels' ``channel_group`` to NULL ("No Group").
* ``upsert_epg``: create/update ``EPGData`` rows by ``(tvg_id, epg_source)`` and replace their
  ``ProgramData`` inside one time window, only in EPG sources the operator listed.

What it will never do: read data back, delete a channel, stream, group, source or guide entry,
run a task, or log the secret, a MAC or a payload.
"""

import collections
import hashlib
import hmac
import logging
import math
import os
import re
import threading
import time
from datetime import datetime, timezone

VERSION = "0.1.0"
PROTOCOL = 1

OPS = (
    "ping",
    "set_stream_lists",
    "set_stream_stats",
    "clear_channel_groups",
    "upsert_epg",
)

# How far a request's timestamp may be from this server's clock, either way.
REPLAY_WINDOW_SECS = 300
MIN_SECRET_CHARS = 32

# The hardened route: secrets from Dispatcharr's environment, which keeps them out of the
# database and out of GET /api/plugins/plugins/ (that listing returns every plugin's settings).
# Each variable, when set and not blank, wins over the matching setting.
ENV_SECRET = "CROSSTALK_BRIDGE_SECRET"
ENV_PREVIOUS_SECRET = "CROSSTALK_BRIDGE_PREVIOUS_SECRET"
ENV_GRACE_UNTIL = "CROSSTALK_BRIDGE_GRACE_UNTIL"

LIMITS = {
    # set_stream_lists: channels per request, streams in one channel's list, and stream ids
    # across the whole request.
    "max_channels": 2000,
    "max_list_len": 1000,
    "max_stream_refs": 20000,
    # set_stream_stats: streams per request, and canonical bytes of one stats object.
    "max_streams": 5000,
    "max_stats_bytes": 16384,
    # clear_channel_groups uses max_channels.
    # upsert_epg: entries and programmes per request, and the widest window in seconds.
    "max_epg_entries": 2000,
    "max_programs": 50000,
    "max_window_secs": 14 * 24 * 3600,
}

# Integers outside +-(2^53 - 1) cannot be represented the same way in every JSON reader.
MAX_SAFE_INTEGER = 2**53 - 1
MAX_DEPTH = 32

logger = logging.getLogger("plugins.crosstalk_bridge")


# ---------------------------------------------------------------------------------------------
# Canonical JSON (docs/bridge.md, "Canonical JSON"). crosstalk implements the same function in
# crates/crosstalk-core/src/bridge.rs; bridge/tests/vectors.json pins both.
# ---------------------------------------------------------------------------------------------


class CanonicalError(ValueError):
    """A value that has no canonical form (NaN, a huge integer, a lone surrogate...)."""


_ESCAPES = {
    '"': '\\"',
    "\\": "\\\\",
    "\b": "\\b",
    "\t": "\\t",
    "\n": "\\n",
    "\f": "\\f",
    "\r": "\\r",
}


def canonical_json(value):
    """The canonical JSON text of ``value``: sorted keys, no whitespace, minimal escapes."""
    out = []
    _encode(value, out, 0)
    return "".join(out)


def _encode(value, out, depth):
    if depth > MAX_DEPTH:
        raise CanonicalError("nested deeper than %d levels" % MAX_DEPTH)
    if value is None:
        out.append("null")
    elif value is True:
        out.append("true")
    elif value is False:
        out.append("false")
    elif isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise CanonicalError("integer outside +-(2^53 - 1)")
        out.append(str(int(value)))
    elif isinstance(value, float):
        out.append(_number(value))
    elif isinstance(value, str):
        out.append(_string(value))
    elif isinstance(value, (list, tuple)):
        out.append("[")
        for index, item in enumerate(value):
            if index:
                out.append(",")
            _encode(item, out, depth + 1)
        out.append("]")
    elif isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                raise CanonicalError("object keys must be strings")
        out.append("{")
        for index, key in enumerate(sorted(value)):
            if index:
                out.append(",")
            out.append(_string(key))
            out.append(":")
            _encode(value[key], out, depth + 1)
        out.append("}")
    else:
        raise CanonicalError("%s has no JSON form" % type(value).__name__)


def _string(text):
    parts = ['"']
    for char in text:
        code = ord(char)
        if char in _ESCAPES:
            parts.append(_ESCAPES[char])
        elif code < 0x20:
            parts.append("\\u%04x" % code)
        elif 0xD800 <= code <= 0xDFFF:
            raise CanonicalError("lone surrogate in a string")
        else:
            parts.append(char)
    parts.append('"')
    return "".join(parts)


def _number(number):
    """A finite float as ECMAScript's Number.prototype.toString writes it (RFC 8785 3.2.2.3)."""
    if math.isnan(number) or math.isinf(number):
        raise CanonicalError("NaN and infinities have no JSON form")
    if number == 0:
        return "0"
    sign = "-" if number < 0 else ""
    # repr() is the shortest text that reads back as the same double; only its layout differs
    # from ECMAScript's, so take its digits and exponent and lay them out again.
    mantissa, _, exponent = repr(abs(number)).partition("e")
    whole, _, fraction = mantissa.partition(".")
    digits = whole + fraction
    # value = 0.<digits> x 10^point
    point = len(whole) + (int(exponent) if exponent else 0)
    stripped = digits.lstrip("0")
    point -= len(digits) - len(stripped)
    digits = stripped.rstrip("0")
    return sign + _layout(digits, point)


def _layout(digits, point):
    count = len(digits)
    if count <= point <= 21:
        return digits + "0" * (point - count)
    if 0 < point <= 21:
        return digits[:point] + "." + digits[point:]
    if -6 < point <= 0:
        return "0." + "0" * (-point) + digits
    exponent = point - 1
    head = digits[0] + ("." + digits[1:] if count > 1 else "")
    return head + "e" + ("+" if exponent > 0 else "-") + str(abs(exponent))


def sign(secret, envelope):
    """The lowercase hex HMAC-SHA256 of the envelope's canonical JSON under ``secret``."""
    message = canonical_json(envelope).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


# ---------------------------------------------------------------------------------------------
# Verification.
# ---------------------------------------------------------------------------------------------


class Refused(Exception):
    """The whole request is refused; ``code`` is the stable reason, ``detail`` says why."""

    def __init__(self, code, detail=""):
        super().__init__(code)
        self.code = code
        self.detail = detail

    def reason(self):
        return "%s: %s" % (self.code, self.detail) if self.detail else self.code


_HEX_MAC = re.compile(r"^[0-9a-f]{64}$")
_HEX_NONCE = re.compile(r"^[0-9a-f]{32}$")
_ENVELOPE_KEYS = {"v", "ts", "nonce", "op", "payload"}


class NonceCache:
    """Nonces seen inside the replay window, oldest first, bounded.

    Per process: Dispatcharr runs several workers, so the shared Django cache is asked first
    when there is one (see ``_shared_nonce_is_new``). A nonce leaves this cache once its
    timestamp is outside the window, because the timestamp check refuses it from then on.
    """

    def __init__(self, capacity=200000):
        self._seen = collections.OrderedDict()
        self._capacity = capacity
        self._lock = threading.Lock()

    def add(self, nonce, expires_at, now):
        """Record ``nonce``; False when it was already recorded and has not expired."""
        with self._lock:
            while self._seen:
                oldest, expiry = next(iter(self._seen.items()))
                if expiry > now and len(self._seen) < self._capacity:
                    break
                del self._seen[oldest]
            if nonce in self._seen and self._seen[nonce] > now:
                return False
            self._seen[nonce] = expires_at
            return True


def _shared_nonce_is_new(nonce, ttl):
    """Ask Django's cache (shared by every worker when it is Redis) to record the nonce.

    True: recorded now. False: already there (a replay). None: no usable shared cache, so the
    per-process cache is all there is.
    """
    try:
        from django.core.cache import cache
    except Exception:
        return None
    try:
        return bool(cache.add("crosstalk_bridge:nonce:" + nonce, 1, ttl))
    except Exception:
        return None


def _parse_instant(text):
    """Unix seconds from an ISO 8601 time (``Z`` or an offset; naive means UTC) or digits."""
    text = (text or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{1,12}", text):
        return int(text)
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp())


def _setting(settings, variable, key):
    """``(value, source)``: the environment variable when set and not blank, else the setting."""
    value = os.environ.get(variable, "").strip()
    if value:
        return value, "env"
    value = str(settings.get(key) or "").strip()
    return value, ("settings" if value else "none")


def secret_source(settings):
    """Where the current secret comes from: ``env``, ``settings`` or ``none``. Never the value."""
    return _setting(settings, ENV_SECRET, "secret")[1]


def _secrets(settings, now):
    """``[(name, secret)]`` this request may be signed with, current first."""
    current, source = _setting(settings, ENV_SECRET, "secret")
    if len(current) < MIN_SECRET_CHARS:
        raise Refused(
            "not_configured",
            "the secret is empty or shorter than %d characters (configured: %s)"
            % (MIN_SECRET_CHARS, source),
        )
    keys = [("current", current)]
    previous, _ = _setting(settings, ENV_PREVIOUS_SECRET, "previous_secret")
    until = _parse_instant(_setting(settings, ENV_GRACE_UNTIL, "grace_until")[0])
    if previous and len(previous) >= MIN_SECRET_CHARS and until is not None and now < until:
        keys.append(("previous", previous))
    return keys


def grace_state(settings, now):
    previous, _ = _setting(settings, ENV_PREVIOUS_SECRET, "previous_secret")
    if not previous:
        return "none"
    until = _parse_instant(_setting(settings, ENV_GRACE_UNTIL, "grace_until")[0])
    if until is None:
        return "invalid"
    return "active" if now < until else "expired"


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def verify(action, params, settings, now, nonces):
    """Check a request; answer ``(envelope, key_name)`` or raise :class:`Refused`.

    Order: shape, configuration, MAC, timestamp, operation, nonce. The nonce is recorded only
    once the MAC holds, so an unsigned flood cannot fill the cache, and before anything runs,
    so a request that fails validation still cannot be replayed.
    """
    if not isinstance(params, dict) or set(params) != {"envelope", "mac"}:
        raise Refused("bad_request", "params must be exactly {envelope, mac}")
    envelope = params["envelope"]
    mac = params["mac"]
    if not isinstance(mac, str) or not _HEX_MAC.match(mac):
        raise Refused("bad_request", "mac must be 64 lowercase hex digits")
    if not isinstance(envelope, dict) or set(envelope) != _ENVELOPE_KEYS:
        raise Refused("bad_request", "envelope must have exactly v, ts, nonce, op, payload")

    keys = _secrets(settings, now)
    try:
        message = canonical_json(envelope).encode("utf-8")
    except CanonicalError as error:
        raise Refused("bad_request", "envelope has no canonical form: %s" % error)
    matched = None
    for name, secret in keys:
        expected = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
        # Every key is compared, so the time taken does not say which one matched.
        if hmac.compare_digest(expected, mac) and matched is None:
            matched = name
    if matched is None:
        raise Refused("bad_mac", "the signature does not match the configured secret")

    if envelope["v"] != PROTOCOL or not _is_int(envelope["v"]):
        raise Refused("bad_request", "protocol version %r is not %d" % (envelope["v"], PROTOCOL))
    ts = envelope["ts"]
    if not _is_int(ts):
        raise Refused("bad_request", "ts must be an integer")
    if abs(now - ts) > REPLAY_WINDOW_SECS:
        raise Refused(
            "stale",
            "the request is %d s from this server's clock (window %d s)"
            % (int(now - ts), REPLAY_WINDOW_SECS),
        )
    op = envelope["op"]
    if op != action:
        raise Refused("bad_request", "the signed op does not match the action")
    if op not in OPS:
        raise Refused("unknown_op", "%r is not one of %s" % (op, ", ".join(OPS)))
    nonce = envelope["nonce"]
    if not isinstance(nonce, str) or not _HEX_NONCE.match(nonce):
        raise Refused("bad_request", "nonce must be 32 lowercase hex digits")
    ttl = 2 * REPLAY_WINDOW_SECS
    shared = _shared_nonce_is_new(nonce, ttl)
    local = nonces.add(nonce, now + ttl, now)
    if shared is False or not local:
        raise Refused("replayed", "this nonce was already used")
    if not isinstance(envelope["payload"], dict):
        raise Refused("bad_request", "payload must be an object")
    return envelope, matched


# ---------------------------------------------------------------------------------------------
# Payload validation: shape and limits, before anything touches the database.
# ---------------------------------------------------------------------------------------------


def _require_keys(payload, required, optional=()):
    keys = set(payload)
    missing = set(required) - keys
    extra = keys - set(required) - set(optional)
    if missing or extra:
        raise Refused(
            "invalid",
            "expected keys %s%s"
            % (
                ", ".join(sorted(required)),
                (" (optional %s)" % ", ".join(sorted(optional))) if optional else "",
            ),
        )


def _ids(values, what, limit):
    if not isinstance(values, list):
        raise Refused("invalid", "%s must be a list" % what)
    if len(values) > limit:
        raise Refused("too_large", "%d %s, limit %d" % (len(values), what, limit))
    for value in values:
        if not _is_int(value) or value <= 0:
            raise Refused("invalid", "%s must be positive integers" % what)
    if len(set(values)) != len(values):
        raise Refused("invalid", "%s repeats an id" % what)
    return values


def _text(value, what, limit, nullable=False, empty=True):
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise Refused("invalid", "%s must be a string" % what)
    if len(value) > limit or (not empty and not value.strip()):
        raise Refused("invalid", "%s must be %s%d characters at most" % (
            what, "" if empty else "non-empty and ", limit))
    return value


def validate_set_stream_lists(payload):
    _require_keys(payload, {"lists"})
    lists = payload["lists"]
    if not isinstance(lists, list):
        raise Refused("invalid", "lists must be a list")
    if len(lists) > LIMITS["max_channels"]:
        raise Refused("too_large", "%d channels, limit %d" % (len(lists), LIMITS["max_channels"]))
    seen = set()
    refs = 0
    out = []
    for item in lists:
        if not isinstance(item, dict):
            raise Refused("invalid", "every list is {channel_id, stream_ids}")
        _require_keys(item, {"channel_id", "stream_ids"})
        channel_id = item["channel_id"]
        if not _is_int(channel_id) or channel_id <= 0:
            raise Refused("invalid", "channel_id must be a positive integer")
        if channel_id in seen:
            raise Refused("invalid", "channel %d is listed twice" % channel_id)
        seen.add(channel_id)
        streams = _ids(item["stream_ids"], "stream_ids", LIMITS["max_list_len"])
        refs += len(streams)
        out.append((channel_id, list(streams)))
    if refs > LIMITS["max_stream_refs"]:
        raise Refused("too_large", "%d stream ids, limit %d" % (refs, LIMITS["max_stream_refs"]))
    return out


def validate_set_stream_stats(payload):
    _require_keys(payload, {"stats"})
    rows = payload["stats"]
    if not isinstance(rows, list):
        raise Refused("invalid", "stats must be a list")
    if len(rows) > LIMITS["max_streams"]:
        raise Refused("too_large", "%d streams, limit %d" % (len(rows), LIMITS["max_streams"]))
    seen = set()
    out = []
    for item in rows:
        if not isinstance(item, dict):
            raise Refused("invalid", "every row is {stream_id, stream_stats, updated_at}")
        _require_keys(item, {"stream_id", "stream_stats", "updated_at"})
        stream_id = item["stream_id"]
        if not _is_int(stream_id) or stream_id <= 0:
            raise Refused("invalid", "stream_id must be a positive integer")
        if stream_id in seen:
            raise Refused("invalid", "stream %d is listed twice" % stream_id)
        seen.add(stream_id)
        stats = item["stream_stats"]
        if stats is not None and not isinstance(stats, dict):
            raise Refused("invalid", "stream_stats must be an object or null")
        if len(canonical_json(stats).encode("utf-8")) > LIMITS["max_stats_bytes"]:
            raise Refused("too_large", "stream %d's stats exceed %d bytes" % (
                stream_id, LIMITS["max_stats_bytes"]))
        updated_at = item["updated_at"]
        if not _is_int(updated_at) or updated_at < 0:
            raise Refused("invalid", "updated_at must be unix seconds")
        out.append((stream_id, stats, updated_at))
    return out


def validate_clear_channel_groups(payload):
    _require_keys(payload, {"channel_ids"})
    return list(_ids(payload["channel_ids"], "channel_ids", LIMITS["max_channels"]))


def validate_upsert_epg(payload):
    _require_keys(payload, {"epg_source_id", "window", "entries"})
    source_id = payload["epg_source_id"]
    if not _is_int(source_id) or source_id <= 0:
        raise Refused("invalid", "epg_source_id must be a positive integer")
    window = payload["window"]
    if not isinstance(window, dict):
        raise Refused("invalid", "window must be {start, end}")
    _require_keys(window, {"start", "end"})
    start, end = window["start"], window["end"]
    if not (_is_int(start) and _is_int(end)) or start < 0 or end <= start:
        raise Refused("invalid", "window must be unix seconds with start < end")
    if end - start > LIMITS["max_window_secs"]:
        raise Refused("too_large", "the window is wider than %d s" % LIMITS["max_window_secs"])
    entries = payload["entries"]
    if not isinstance(entries, list):
        raise Refused("invalid", "entries must be a list")
    if len(entries) > LIMITS["max_epg_entries"]:
        raise Refused("too_large", "%d entries, limit %d" % (
            len(entries), LIMITS["max_epg_entries"]))
    programs_total = 0
    seen = set()
    out = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise Refused("invalid", "every entry is {tvg_id, name, icon_url, programs}")
        _require_keys(entry, {"tvg_id", "name", "icon_url", "programs"})
        tvg_id = _text(entry["tvg_id"], "tvg_id", 255, empty=False)
        if tvg_id in seen:
            raise Refused("invalid", "tvg_id %r is listed twice" % tvg_id)
        seen.add(tvg_id)
        name = _text(entry["name"], "name", 512, empty=False)
        icon_url = _text(entry["icon_url"], "icon_url", 500, nullable=True)
        programs = entry["programs"]
        if not isinstance(programs, list):
            raise Refused("invalid", "programs must be a list")
        programs_total += len(programs)
        if programs_total > LIMITS["max_programs"]:
            raise Refused("too_large", "more than %d programmes" % LIMITS["max_programs"])
        checked = []
        problem = None
        for program in programs:
            if not isinstance(program, dict):
                raise Refused("invalid", "every programme is an object")
            _require_keys(
                program,
                {"start", "end", "title"},
                {"sub_title", "description", "custom_properties"},
            )
            p_start, p_end = program["start"], program["end"]
            if not (_is_int(p_start) and _is_int(p_end)) or p_end <= p_start:
                raise Refused("invalid", "a programme needs integer start < end")
            title = _text(program["title"], "title", 255)
            sub_title = _text(program.get("sub_title"), "sub_title", 100000, nullable=True)
            description = _text(program.get("description"), "description", 100000, nullable=True)
            custom = program.get("custom_properties")
            if custom is not None and not isinstance(custom, dict):
                raise Refused("invalid", "custom_properties must be an object or null")
            if not start <= p_start < end:
                # Attributed to the entry, which is then left alone: replacing its window with
                # a programme outside it would leave the old one behind.
                problem = "invalid: a programme starts outside the window"
            checked.append((p_start, p_end, title, sub_title, description, custom))
        out.append((tvg_id, name, icon_url, checked, problem))
    return source_id, (start, end), out


VALIDATORS = {
    "ping": lambda payload: _require_keys(payload, set()),
    "set_stream_lists": validate_set_stream_lists,
    "set_stream_stats": validate_set_stream_stats,
    "clear_channel_groups": validate_clear_channel_groups,
    "upsert_epg": validate_upsert_epg,
}


# ---------------------------------------------------------------------------------------------
# The writes. Each answers (applied, skipped, errors) and runs inside one transaction the
# caller opens. Every id the request named lands in exactly one place: counted in applied,
# listed in skipped (already in the requested state), or listed in errors (left untouched).
# ---------------------------------------------------------------------------------------------


def _utc(seconds):
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


class OrmStore:
    """The Django ORM writes. Imports happen on use, so the module loads without Django."""

    def atomic(self):
        from django.db import transaction

        return transaction.atomic()

    def set_stream_lists(self, lists):
        from apps.channels.models import Channel, ChannelStream, Stream

        channel_ids = [channel_id for channel_id, _ in lists]
        known_channels = set(
            Channel.objects.filter(id__in=channel_ids).values_list("id", flat=True)
        )
        wanted_streams = sorted({stream for _, streams in lists for stream in streams})
        known_streams = set(
            Stream.objects.filter(id__in=wanted_streams).values_list("id", flat=True)
        )
        errors = []
        writable = []
        for channel_id, streams in lists:
            if channel_id not in known_channels:
                errors.append({"id": channel_id, "reason": "not_found: no such channel"})
                continue
            missing = [stream for stream in streams if stream not in known_streams]
            if missing:
                errors.append({
                    "id": channel_id,
                    "reason": "not_found: no stream %s" % ", ".join(str(s) for s in missing[:10]),
                })
                continue
            writable.append((channel_id, streams))

        current = collections.defaultdict(list)
        if writable:
            rows = ChannelStream.objects.filter(
                channel_id__in=[channel_id for channel_id, _ in writable]
            ).values_list("id", "channel_id", "stream_id", "order")
            for row_id, channel_id, stream_id, order in rows:
                current[channel_id].append((order, row_id, stream_id))

        delete_ids = []
        reorder = []
        create = []
        applied = 0
        skipped = []
        changed_channels = []
        for channel_id, streams in writable:
            existing = sorted(current.get(channel_id, []))
            if [stream_id for _, _, stream_id in existing] == streams and [
                order for order, _, _ in existing
            ] == list(range(len(streams))):
                skipped.append(channel_id)
                continue
            by_stream = {stream_id: (row_id, order) for order, row_id, stream_id in existing}
            wanted = set(streams)
            delete_ids.extend(
                row_id for _, row_id, stream_id in existing if stream_id not in wanted
            )
            for position, stream_id in enumerate(streams):
                if stream_id in by_stream:
                    row_id, order = by_stream[stream_id]
                    if order != position:
                        reorder.append(
                            ChannelStream(id=row_id, channel_id=channel_id,
                                          stream_id=stream_id, order=position)
                        )
                else:
                    create.append(
                        ChannelStream(channel_id=channel_id, stream_id=stream_id, order=position)
                    )
            applied += 1
            changed_channels.append(channel_id)

        # Removed rows first: (channel, stream) is unique, and a kept stream keeps its row.
        if delete_ids:
            ChannelStream.objects.filter(id__in=delete_ids).delete()
        if reorder:
            ChannelStream.objects.bulk_update(reorder, ["order"], batch_size=1000)
        if create:
            ChannelStream.objects.bulk_create(create, batch_size=1000)
        if changed_channels:
            _rollup_catchup(changed_channels)
        return applied, skipped, errors

    def set_stream_stats(self, rows):
        from apps.channels.models import Stream

        ids = [stream_id for stream_id, _, _ in rows]
        streams = {
            stream.id: stream
            for stream in Stream.objects.filter(id__in=ids).only(
                "id", "stream_stats", "stream_stats_updated_at"
            )
        }
        errors = []
        skipped = []
        changed = []
        for stream_id, stats, updated_at in rows:
            stream = streams.get(stream_id)
            if stream is None:
                errors.append({"id": stream_id, "reason": "not_found: no such stream"})
                continue
            when = _utc(updated_at)
            if stream.stream_stats == stats and stream.stream_stats_updated_at == when:
                skipped.append(stream_id)
                continue
            stream.stream_stats = stats
            stream.stream_stats_updated_at = when
            changed.append(stream)
        if changed:
            Stream.objects.bulk_update(
                changed, ["stream_stats", "stream_stats_updated_at"], batch_size=500
            )
        return len(changed), skipped, errors

    def clear_channel_groups(self, channel_ids):
        from apps.channels.models import Channel

        current = dict(
            Channel.objects.filter(id__in=channel_ids).values_list("id", "channel_group_id")
        )
        errors = []
        skipped = []
        clear = []
        for channel_id in channel_ids:
            if channel_id not in current:
                errors.append({"id": channel_id, "reason": "not_found: no such channel"})
            elif current[channel_id] is None:
                skipped.append(channel_id)
            else:
                clear.append(channel_id)
        if clear:
            Channel.objects.filter(id__in=clear).update(channel_group=None)
        return len(clear), skipped, errors

    def upsert_epg(self, source_id, window, entries):
        from apps.epg.models import EPGData, EPGSource, ProgramData

        if not EPGSource.objects.filter(id=source_id).exists():
            raise Refused("not_found", "no EPG source %d" % source_id)
        errors = []
        writable = []
        for tvg_id, name, icon_url, programs, problem in entries:
            if problem:
                errors.append({"id": tvg_id, "reason": problem})
            else:
                writable.append((tvg_id, name, icon_url, programs))
        existing = {
            row.tvg_id: row
            for row in EPGData.objects.filter(
                epg_source_id=source_id, tvg_id__in=[entry[0] for entry in writable]
            )
        }
        renamed = []
        rows = {}
        for tvg_id, name, icon_url, _ in writable:
            row = existing.get(tvg_id)
            if row is None:
                row = EPGData.objects.create(
                    tvg_id=tvg_id, name=name, icon_url=icon_url, epg_source_id=source_id
                )
            elif row.name != name or row.icon_url != icon_url:
                row.name = name
                row.icon_url = icon_url
                renamed.append(row)
            rows[tvg_id] = row
        if renamed:
            EPGData.objects.bulk_update(renamed, ["name", "icon_url"], batch_size=500)

        start, end = window
        if rows:
            # The replace: this window of these entries, nothing else.
            ProgramData.objects.filter(
                epg_id__in=[row.id for row in rows.values()],
                start_time__gte=_utc(start),
                start_time__lt=_utc(end),
            ).delete()
        programs = []
        for tvg_id, _, _, entry_programs in writable:
            row = rows[tvg_id]
            for p_start, p_end, title, sub_title, description, custom in entry_programs:
                programs.append(
                    ProgramData(
                        epg_id=row.id,
                        start_time=_utc(p_start),
                        end_time=_utc(p_end),
                        title=title,
                        sub_title=sub_title,
                        description=description,
                        tvg_id=tvg_id,
                        custom_properties=custom if custom is not None else {},
                    )
                )
        if programs:
            ProgramData.objects.bulk_create(programs, batch_size=1000)
        return len(writable), [], errors


def _rollup_catchup(channel_ids):
    """Dispatcharr's own catch-up roll-up, which a bulk ChannelStream write skips."""
    try:
        from apps.channels.utils import rollup_catchup_for_channels
    except Exception:
        return
    rollup_catchup_for_channels(channel_ids)


def _allowed_sources(settings):
    allowed = set()
    for part in str(settings.get("epg_sources") or "").replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            allowed.add(int(part))
    return allowed


# ---------------------------------------------------------------------------------------------
# The plugin.
# ---------------------------------------------------------------------------------------------


class Plugin:
    name = "crosstalk bridge"
    version = VERSION
    description = (
        "The four bulk writes crosstalk needs that Dispatcharr's REST API cannot do. Every "
        "request is HMAC-signed."
    )
    # Fields come from plugin.json; there are no actions to click. Every operation needs a
    # signed request, so a button could only ever fail.
    actions = []

    def __init__(self, store=None, clock=None):
        self._store = store or OrmStore()
        self._clock = clock or time.time
        self._nonces = NonceCache()

    def run(self, action, params, context):
        settings = (context or {}).get("settings") or {}
        now = int(self._clock())
        op = action if action in OPS else None
        try:
            envelope, key = verify(action, params, settings, now, self._nonces)
            op = envelope["op"]
            response = self._dispatch(op, envelope["payload"], settings, now, key)
        except Refused as refused:
            # The reason code and the op only: never the secret, the MAC or the payload.
            logger.warning("crosstalk bridge refused %s: %s", op or "a request", refused.code)
            return _answer(op, ok=False, errors=[{"id": None, "reason": refused.reason()}])
        except Exception as error:  # noqa: BLE001 - the transaction rolled back; say so
            logger.exception("crosstalk bridge %s failed; nothing was written", op)
            return _answer(op, ok=False, errors=[{
                "id": None,
                "reason": "internal: %s (nothing was written)" % type(error).__name__,
            }])
        logger.info(
            "crosstalk bridge %s: %d applied, %d unchanged, %d failed",
            op, response["applied"], len(response["skipped"]), len(response["errors"]),
        )
        return response

    def _dispatch(self, op, payload, settings, now, key):
        checked = VALIDATORS[op](payload)
        if op == "ping":
            answer = _answer(op, ok=True)
            answer.update({
                "protocol": PROTOCOL,
                "capabilities": list(OPS),
                "limits": dict(LIMITS),
                "key": key,
                "configured": secret_source(settings),
                "grace": grace_state(settings, now),
                "epg_sources": sorted(_allowed_sources(settings)),
            })
            return answer
        if op == "upsert_epg":
            source_id = checked[0]
            if source_id not in _allowed_sources(settings):
                raise Refused(
                    "not_allowed",
                    "EPG source %d is not in this plugin's epg_sources setting" % source_id,
                )
        with self._store.atomic():
            if op == "set_stream_lists":
                applied, skipped, errors = self._store.set_stream_lists(checked)
            elif op == "set_stream_stats":
                applied, skipped, errors = self._store.set_stream_stats(checked)
            elif op == "clear_channel_groups":
                applied, skipped, errors = self._store.clear_channel_groups(checked)
            else:
                applied, skipped, errors = self._store.upsert_epg(*checked)
        return _answer(op, ok=True, applied=applied, skipped=skipped, errors=errors)


def _answer(op, ok, applied=0, skipped=None, errors=None):
    return {
        "ok": ok,
        "op": op,
        "applied": applied,
        "skipped": list(skipped or []),
        "errors": list(errors or []),
        "version": VERSION,
    }
