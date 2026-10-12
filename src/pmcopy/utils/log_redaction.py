"""Single emit-time secret-redaction pass shared by every logging sink.

Design (A07, standalone-refactor task 5):

- **Value-based redaction** — a process-wide registry of known secret
  *values* (the configured wallet private key, the Falcon analytics token,
  the Relayer API key, the database DSN).
  Registration happens once, after configuration load, via
  :func:`register_config_secrets` (called from ``app.py`` right after
  ``load_config()`` — the composition point where the validated config is
  available and before any component logs). The registry never logs what
  it holds and never raises.
- **Field-name-based redaction** — dict keys / log-record extra names that
  are credential-shaped (``private_key``, ``api_key``, ``client_secret``,
  ``password``, ``authorization``, exact ``token``, ``*_token``
  compounds, …) are replaced with ``***REDACTED***`` regardless of the
  value. Public market identity is explicitly exempt: keys ending in
  ``_id``/``_ids`` (``token_id``, ``condition_id``, ``pruned_token_ids``,
  …) are identifiers, not credentials, and stay readable.
- **Pattern redaction of free text** — ``Authorization: <credentials>``
  headers (quoted or bare, to end-of-value), ``Bearer <token>``,
  ``apikey=``/``api_key=``/``token=``/… URL query parameters, URL
  userinfo passwords (``scheme://user:pass@host``), provider path keys
  (``/v2/<32-char key>`` RPC/WSS endpoints), and ``secret=…``-style
  assignments in rendered strings — with non-secret URL structure kept
  readable.

The module is safe by default: with an empty value registry (e.g. the
implicit ``configure_logging()`` performed by ``get_logger()`` before any
config is loaded), field-name and pattern redaction are still fully
active. Value registration exists to catch secrets that appear in
otherwise innocent positions (free-text messages, exception args,
object reprs).

This module must stay dependency-free (stdlib + typing only) so any
layer, including config loading itself, can import it safely.
"""

from __future__ import annotations

import re
from typing import Any

__all__ = [
    "REDACTED_PLACEHOLDER",
    "is_secret_key",
    "redact_object",
    "redact_text",
    "register_config_secrets",
    "register_secret_values",
    "reset_secret_registry",
    "registered_secret_count",
]

REDACTED_PLACEHOLDER = "***REDACTED***"

# Registered values shorter than this are ignored: replacing a very short
# string everywhere would redact unrelated substrings of ordinary text.
# Every secret this registry is populated from (private keys, API tokens,
# bearer credentials, relayer keys, the DSN) is far longer than this floor.
_MIN_REGISTERED_VALUE_LENGTH = 6

# ---------------------------------------------------------------------------
# Value registry
# ---------------------------------------------------------------------------

_VALUE_REGISTRY: set[str] = set()
_SORTED_VALUES: tuple[str, ...] = ()


def _plain_text(value: object) -> str:
    """Return the raw string behind ``value`` (SecretStr aware); else ``''``."""
    if value is None:
        return ""
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        try:
            value = getter()
        except Exception:
            return ""
    if isinstance(value, str):
        return value.strip()
    return ""


def _rebuild_snapshot() -> None:
    # Longest first so overlapping secrets cannot mask each other.
    global _SORTED_VALUES
    _SORTED_VALUES = tuple(sorted(_VALUE_REGISTRY, key=len, reverse=True))


def register_secret_values(*values: object) -> int:
    """Register known secret values for value-based text redaction.

    Accepts plain strings and ``SecretStr`` instances; empty, whitespace,
    or very short values are ignored. Never raises, never logs. Returns
    the number of newly registered values.
    """
    added = 0
    try:
        for value in values:
            text = _plain_text(value)
            if len(text) < _MIN_REGISTERED_VALUE_LENGTH:
                continue
            if text not in _VALUE_REGISTRY:
                _VALUE_REGISTRY.add(text)
                added += 1
        _rebuild_snapshot()
    except Exception:
        pass
    return added


def reset_secret_registry() -> int:
    """Clear the registry (test / reconfiguration hook). Returns prior size."""
    count = len(_VALUE_REGISTRY)
    _VALUE_REGISTRY.clear()
    _rebuild_snapshot()
    return count


def registered_secret_count() -> int:
    """Return how many secret values are currently registered."""
    return len(_VALUE_REGISTRY)


def register_config_secrets(config: object) -> int:
    """Register every secret carried by a validated ``AppConfig``.

    Duck-typed (``getattr`` walks) so this module needs no config import
    and broken/partial objects simply register nothing. Source fields:

    - ``wallet.private_key`` — EOA signing key,
    - ``database.dsn`` — embeds the Postgres password,
    - ``discovery.api.token`` — Falcon analytics bearer token,
    - ``redemption.relayer_api_key`` — Relayer API key.

    RPC/WSS endpoint URLs are deliberately NOT registered: their public
    defaults must stay readable in logs; keys embedded in such URLs are
    covered by the ``/v2/<key>`` path and query-parameter patterns.

    Never raises, never logs. Returns the number of registered values.
    """
    try:
        candidates: list[object] = []
        wallet = getattr(config, "wallet", None)
        candidates.append(getattr(wallet, "private_key", None))
        database = getattr(config, "database", None)
        candidates.append(getattr(database, "dsn", None))
        discovery_api = getattr(getattr(config, "discovery", None), "api", None)
        candidates.append(getattr(discovery_api, "token", None))
        redemption = getattr(config, "redemption", None)
        candidates.append(getattr(redemption, "relayer_api_key", None))
        return register_secret_values(*[c for c in candidates if c is not None])
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Credential-shaped key detection
# ---------------------------------------------------------------------------

# camelCase -> snake_case ("apiKey" -> "api_key"); SCREAMING_SNAKE stays
# intact because the split needs a lowercase/digit before the capital.
_CAMEL_SPLIT_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _normalize_key(key: object) -> str:
    try:
        text = str(key)
    except Exception:
        return ""
    spaced = _CAMEL_SPLIT_RE.sub("_", text)
    return _NON_ALNUM_RE.sub("_", spaced.lower()).strip("_")


# Full-key credential patterns (matched against the whole normalized key).
_CREDENTIAL_KEY_RE = re.compile(
    r"^(?:"
    r"private_?key|priv_?key"
    r"|api_?key|api_?secret|api_?token|api_?pass(?:word|phrase)?"
    r"|secret|password|passwd|passphrase"
    r"|credential|credentials|creds"
    r"|token"
    r"|(?:access|session|refresh|auth|bearer|viewer|operator|id)_?token"
    r"|authorization"
    r"|auth_?header"
    r"|bearer_?cred(?:ential)?s?"
    r"|relayer_?key"
    r"|client_?key"
    r")$"
)

# Single segments that mark a key as credential-bearing even when embedded
# in a longer name (e.g. ``postgres_password``, ``wallet_private_key``).
# ``token`` is deliberately NOT here: ``token_delay_overrides`` and friends
# are market plumbing, and the exact key ``token`` is handled above.
_STRONG_SECRET_SEGMENTS = frozenset(
    {
        "secret",
        "password",
        "passwd",
        "passphrase",
        "credential",
        "credentials",
        "creds",
        "authorization",
        "apikey",
    }
)


def is_secret_key(key: object) -> bool:
    """Return whether a field name is credential-shaped and must be redacted.

    Public market identity is exempt by rule: normalized keys ending in
    ``_id``/``_ids`` (``token_id``, ``condition_id``, ``pruned_token_ids``)
    are identifiers, never credentials.
    """
    normalized = _normalize_key(key)
    if not normalized:
        return False
    if normalized.endswith(("_id", "_ids")):
        return False
    if _CREDENTIAL_KEY_RE.match(normalized):
        return True
    segments = [segment for segment in normalized.split("_") if segment]
    if not segments:
        return False
    if any(segment in _STRONG_SECRET_SEGMENTS for segment in segments):
        return True
    if len(segments) >= 2:
        for i in range(len(segments) - 1):
            if _CREDENTIAL_KEY_RE.match(f"{segments[i]}_{segments[i + 1]}"):
                return True
    return False


# ---------------------------------------------------------------------------
# Free-text pattern redaction
# ---------------------------------------------------------------------------

# "Authorization: <creds>" — consume to a closing quote or end of line so a
# quoted header value ("Authorization': 'Basic abc'") is fully covered.
_AUTHORIZATION_HEADER_RE = re.compile(
    r"(?i)\b(authorization)\b['\"]?\s*[:=]\s*['\"]?([^\r\n'\"]*)"
)
_BEARER_RE = re.compile(r"(?i)\b(bearer)\s+[^\s,;'\"]+")
_URL_USERINFO_RE = re.compile(r"(?i)([a-z][a-z0-9+.\-]*://[^/?#\s:]+):([^/?#\s]+)@")
_URL_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&;](?:api_?key|access_?token|auth|token|secret|signature"
    r"|pass(?:word|phrase)?|key)=)([^&\s'\"]+)"
)
_PROVIDER_PATH_KEY_RE = re.compile(r"(?i)(/v\d+/)([A-Za-z0-9][A-Za-z0-9_\-]{15,})")
# "name: value" / "name=value" assignments whose NAME passes the credential
# key predicate (checked per match, so "token_id=0x44…" stays readable).
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b([A-Za-z_][A-Za-z0-9_\-]{1,48})(['\"]?\s*[=:]\s*['\"]?)([^\s,;&}'\"]+)"
)

# Superset prefilter: when none of these fragments appear (and the value
# registry is empty), the text provably cannot be changed by redact_text —
# every redactable construct contains one of these fragments. Deliberately
# broad (bare token/pass/auth/key/cred) because false positives only cost a
# harmless full-pipeline pass.
_TEXT_SCAN_RE = re.compile(
    r"(?i)authorization|bearer|secret|credential"
    r"|api[_-]?(?:key|secret|token|pass)|apikey"
    r"|token|pass|auth|key|cred"
    r"|[?&;]signature="
    r"|://[^/?#\s:]+:[^/?#\s]+@"
    r"|/v\d+/[A-Za-z0-9_\-]{16}"
)


def _redact_assignment(match: re.Match[str]) -> str:
    name = match.group(1)
    if is_secret_key(name):
        return f"{name}{match.group(2)}{REDACTED_PLACEHOLDER}"
    return match.group(0)


def redact_text(text: str) -> str:
    """Return ``text`` with registered secret values and credential patterns removed."""
    if not text:
        return text
    if _SORTED_VALUES:
        for secret in _SORTED_VALUES:
            if secret in text:
                text = text.replace(secret, REDACTED_PLACEHOLDER)
    if not _TEXT_SCAN_RE.search(text):
        return text
    text = _AUTHORIZATION_HEADER_RE.sub(
        lambda m: f"{m.group(1)}: {REDACTED_PLACEHOLDER}" if m.group(2) else m.group(0),
        text,
    )
    text = _BEARER_RE.sub(lambda m: f"{m.group(1)} {REDACTED_PLACEHOLDER}", text)
    text = _URL_USERINFO_RE.sub(
        lambda m: f"{m.group(1)}:{REDACTED_PLACEHOLDER}@", text
    )
    text = _URL_QUERY_SECRET_RE.sub(lambda m: f"{m.group(1)}{REDACTED_PLACEHOLDER}", text)
    text = _PROVIDER_PATH_KEY_RE.sub(lambda m: f"{m.group(1)}{REDACTED_PLACEHOLDER}", text)
    text = _SECRET_ASSIGNMENT_RE.sub(_redact_assignment, text)
    return text


# ---------------------------------------------------------------------------
# Structured-value redaction
# ---------------------------------------------------------------------------


def redact_object(value: Any) -> Any:
    """Deep-redact a structured extra: credential keys and text patterns.

    Returns a new structure for containers (the input is never mutated).
    Non-string scalars pass through unchanged; unknown objects are returned
    as-is so callers keep control of serializability/repr policy.
    """
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {
            key: (REDACTED_PLACEHOLDER if is_secret_key(key) else redact_object(item))
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_object(item) for item in value)
    if isinstance(value, list):
        return [redact_object(item) for item in value]
    if isinstance(value, (set, frozenset)):
        try:
            return type(value)(redact_object(item) for item in value)
        except TypeError:
            return value
    return value
