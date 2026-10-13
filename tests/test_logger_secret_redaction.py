"""A07: real secret redaction across every logging sink.

Contract under test (task brief 2026-10-11 standalone refactor, task 5):

- one emit-time redaction pass covering ALL sinks — the JSONL
  ``JsonFormatter``, the Rich console handler, and the stderr fallback —
  applied to message text, serializable extras (nested values included),
  and exception rendering;
- value-based redaction (registered secret values) plus field-name-based
  redaction for credential-shaped keys, with public market identity
  (``token_id``, ``condition_id``) explicitly exempt;
- credential-bearing URLs/headers (``Authorization: Bearer ...``,
  ``apikey=`` query params) redacted;
- ``safe_error_detail`` honouring its documented no-secret-leak promise;
- ``log_api_response`` never writing the raw ``error`` string into extras;
- order-lifecycle dedup keys account-scoped when no venue order id exists,
  with byte-identical legacy behaviour when no account is supplied.

Every canary below is assembled at runtime from parts; no
credential-shaped literal appears in this file. Canaries are registered
into the value registry (or carried inside pattern-reachable forms) so the
redaction engine can actually find them.
"""

from __future__ import annotations

import io
import json
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from rich.console import Console

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pmcopy.utils import logger as logger_module  # noqa: E402
from pmcopy.utils.logger import (  # noqa: E402
    APP_NAME,
    JsonFormatter,
    configure_logging,
    log_api_error,
    log_api_response,
    log_order_lifecycle,
    safe_error_detail,
    _reset_lifecycle_dedup_for_tests,
)
from pmcopy.utils.log_redaction import (  # noqa: E402
    REDACTED_PLACEHOLDER,
    is_secret_key,
    redact_object,
    redact_text,
    register_secret_values,
    reset_secret_registry,
)


def _canary(tag: str) -> str:
    """Build a synthetic canary at runtime from parts (never a literal)."""
    return "".join(("synthetic-", tag, "-rnd-", "7f3a9c2e8d1b"))


def _close_and_clear_handlers(logger: logging.Logger) -> None:
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()


@pytest.fixture(autouse=True)
def _clean_redaction_state():
    reset_secret_registry()
    _reset_lifecycle_dedup_for_tests()
    root = logging.getLogger(APP_NAME)
    _close_and_clear_handlers(root)
    yield
    reset_secret_registry()
    _reset_lifecycle_dedup_for_tests()
    _close_and_clear_handlers(root)
    logger_module._CONFIGURED_LOGGERS.clear()
    # Restore the standard handler set so cross-file test ordering is stable.
    configure_logging()


@contextmanager
def _captured_console():
    """Swap the module console for a deterministic capture buffer."""
    buffer = io.StringIO()
    captured = Console(
        file=buffer,
        width=100,
        theme=logger_module._THEME,
        no_color=True,
        force_terminal=False,
        legacy_windows=False,
    )
    original = logger_module.console
    logger_module.console = captured
    try:
        yield buffer
    finally:
        logger_module.console = original


def _make_record(msg: str, *args: object, level: int = logging.INFO, **extras) -> logging.LogRecord:
    record = logging.LogRecord(
        name="redaction.test",
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=args or (),
        exc_info=None,
    )
    for key, value in extras.items():
        setattr(record, key, value)
    return record


def _jsonl_rows(tmp_path: Path) -> list[dict]:
    jsonl = tmp_path / f"{APP_NAME}.jsonl"
    text = jsonl.read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _flush(log: logging.Logger) -> None:
    for handler in log.handlers:
        handler.flush()


class TestSecretKeyClassifier:
    """Credential-shaped keys redact; public market identity does not."""

    @pytest.mark.parametrize(
        "key",
        [
            "private_key",
            "PRIVATE_KEY",
            "privateKey",
            "api_key",
            "apiKey",
            "apikey",
            "api_secret",
            "api_token",
            "auth_token",
            "authToken",
            "access_token",
            "refresh_token",
            "session_token",
            "viewer_token",
            "operator_token",
            "client_secret",
            "password",
            "passwd",
            "passphrase",
            "api_passphrase",
            "authorization",
            "Authorization",
            "credentials",
            "relayer_api_key",
            "wallet_private_key",
            "postgres_password",
            "token",
        ],
    )
    def test_credential_keys_are_secret(self, key):
        assert is_secret_key(key) is True

    @pytest.mark.parametrize(
        "key",
        [
            "token_id",
            "token_ids",
            "pruned_token_ids",
            "outcome_token_id",
            "condition_id",
            "conditionId",
            "pruned_condition_ids",
            "asset",
            "asset_id",
            "signatureType",
            "signature_type",
            "size",
            "price",
            "title",
            "eventUrl",
            "order_id",
            "copyOrderId",
            "signal_id",
            "txHash",
            "user_id",
            "leader_id",
            "status",
            "reason",
            "skipCode",
            "venueStatus",
            "submissionState",
            "requestAmount",
            "requestAmountUnit",
            "sizeEstimated",
            "type",
            "error",
            "error_detail",
            "url",
            "httpMethod",
            "httpStatus",
            "response_shape",
            "data_summary",
            "account",
        ],
    )
    def test_public_and_operational_keys_are_not_secret(self, key):
        assert is_secret_key(key) is False


class TestTextPatternRedaction:
    """Pattern-based redaction of credential-bearing URLs and headers."""

    def test_bearer_header_redacted(self):
        canary = _canary("bearer")
        out = redact_text(f"request failed with Authorization: Bearer {canary}")
        assert canary not in out
        assert REDACTED_PLACEHOLDER in out

    def test_authorization_header_value_redacted(self):
        canary = _canary("basic")
        out = redact_text(f"headers={{'Authorization': 'Basic {canary}'}}")
        assert canary not in out

    def test_url_query_apikey_redacted_and_structure_kept(self):
        canary = _canary("query")
        out = redact_text(f"GET https://vendor.test/v1/orders?apikey={canary}&limit=2")
        assert canary not in out
        # Non-secret URL structure stays readable.
        assert "https://vendor.test/v1/orders" in out
        assert "limit=2" in out

    def test_url_query_api_key_and_token_params_redacted(self):
        for param in ("api_key", "token", "auth", "signature", "password"):
            canary = _canary(f"q-{param}")
            out = redact_text(f"https://vendor.test/v1?{param}={canary}&page=1")
            assert canary not in out, param

    def test_url_userinfo_password_redacted(self):
        canary = _canary("userinfo")
        out = redact_text(f"postgres://bot:{canary}@db.internal:5432/trades")
        assert canary not in out
        assert "db.internal:5432/trades" in out

    def test_provider_path_key_redacted(self):
        canary = _canary("alchemy")  # 25 chars -> provider path key shape
        out = redact_text(f"wss://polygon-mainnet.g.alchemy.com/v2/{canary}")
        assert canary not in out

    def test_key_value_assignment_redacted_in_free_text(self):
        canary = _canary("assign")
        out = redact_text(f"order post failed api_key={canary} timeout=3")
        assert canary not in out
        assert "timeout=3" in out

    def test_market_identity_text_untouched(self):
        text = (
            "LIFECYCLE matched | side=BUY | cond=0x3333333333333333333333333333333333333333"
            "33333333333333333333333333 | asset=0x4444444444444444444444444444444444444444"
            "444444444444444444444444 | size=136.3600 price=0.5500"
        )
        assert redact_text(text) == text

    def test_empty_registry_still_redacts_patterns(self):
        # Safe-by-default: patterns work with no registered values.
        canary = _canary("noreg")
        assert canary not in redact_text(f"Authorization: Bearer {canary}")


class TestValueRegistry:
    def test_register_and_redact_registered_value(self):
        canary = _canary("value")
        assert register_secret_values(canary) == 1
        out = redact_text(f"boom while signing with {canary} in flight")
        assert canary not in out
        assert REDACTED_PLACEHOLDER in out

    def test_short_and_empty_values_ignored(self):
        assert register_secret_values("", "   ", "abc") == 0

    def test_secretstr_supported(self):
        from pydantic import SecretStr

        canary = _canary("secretstr")
        assert register_secret_values(SecretStr(canary)) == 1
        assert canary not in redact_text(f"leak {canary}")

    def test_reset_clears_registry(self):
        canary = _canary("reset")
        register_secret_values(canary)
        assert reset_secret_registry() >= 1
        # After reset the value is unknown again (patterns still active).
        assert canary in redact_text(f"opaque {canary}")

    def test_register_config_secrets_collects_all_layers(self):
        from pmcopy.utils.log_redaction import register_config_secrets

        wallet_key = _canary("wallet")
        falcon_token = _canary("falcon")
        relayer_key = _canary("relayer")
        dsn = f"postgresql://bot:{_canary('dsn')}@db.internal:5432/trades"
        config = SimpleNamespace(
            wallet=SimpleNamespace(private_key=wallet_key),
            database=SimpleNamespace(dsn=dsn),
            discovery=SimpleNamespace(api=SimpleNamespace(token=falcon_token)),
            redemption=SimpleNamespace(relayer_api_key=relayer_key),
        )
        count = register_config_secrets(config)
        assert count >= 4
        for value in (wallet_key, falcon_token, relayer_key, dsn):
            assert value not in redact_text(f"context {value} context")

    def test_register_config_secrets_never_raises_on_broken_config(self):
        from pmcopy.utils.log_redaction import register_config_secrets

        assert register_config_secrets(None) == 0
        assert register_config_secrets(SimpleNamespace(wallet=None)) == 0


class TestJsonFormatterRedaction:
    def test_message_with_registered_canary_redacted(self):
        canary = _canary("fmtmsg")
        register_secret_values(canary)
        rendered = JsonFormatter().format(_make_record(f"signing failed with {canary}"))
        assert canary not in rendered
        payload = json.loads(rendered)
        assert REDACTED_PLACEHOLDER in payload["message"]

    def test_credential_key_extra_redacted_without_registry(self):
        canary = _canary("fmtkey")
        rendered = JsonFormatter().format(
            _make_record("event", private_key=canary, api_key=canary)
        )
        assert canary not in rendered
        payload = json.loads(rendered)
        assert payload["private_key"] == REDACTED_PLACEHOLDER
        assert payload["api_key"] == REDACTED_PLACEHOLDER

    def test_innocent_key_with_registered_value_redacted(self):
        canary = _canary("fmtval")
        register_secret_values(canary)
        rendered = JsonFormatter().format(_make_record("event", note=f"ctx {canary}"))
        assert canary not in rendered

    def test_nested_extras_redacted(self):
        canary = _canary("nested")
        register_secret_values(canary)
        record = _make_record(
            "event",
            request={
                "url": f"https://vendor.test/v1/orders?apikey={canary}",
                "headers": {"Authorization": f"Bearer {canary}"},
                "nested": [{"client_secret": canary, "token_id": "0x4444"}],
            },
        )
        rendered = JsonFormatter().format(record)
        assert canary not in rendered
        payload = json.loads(rendered)
        assert REDACTED_PLACEHOLDER in payload["request"]["url"]
        assert REDACTED_PLACEHOLDER in payload["request"]["headers"]["Authorization"]
        assert payload["request"]["nested"][0]["client_secret"] == REDACTED_PLACEHOLDER
        # Market identity inside the same nested structure survives.
        assert payload["request"]["nested"][0]["token_id"] == "0x4444"

    def test_market_identity_extras_survive(self):
        token_id = "0x" + "44" * 32
        condition = "0x" + "33" * 32
        payload = json.loads(
            JsonFormatter().format(
                _make_record("matched", token_id=token_id, conditionId=condition)
            )
        )
        assert payload["token_id"] == token_id
        assert payload["conditionId"] == condition

    def test_exception_text_redacted(self):
        canary = _canary("exc")
        register_secret_values(canary)
        try:
            raise RuntimeError(f"post failed, signed with {canary}")
        except RuntimeError:
            record = _make_record("error", level=logging.ERROR)
            record.exc_info = sys.exc_info()
        payload = json.loads(JsonFormatter().format(record))
        assert canary not in payload["exc_info"]
        assert canary not in payload.get("message", "")
        assert payload["exc_type"] == "RuntimeError"

    def test_exception_object_repr_canary_redacted(self):
        canary = _canary("repr")

        class UnsafePayload:
            def __repr__(self) -> str:  # pragma: no cover - trivial
                return f"<Unsafe {canary}>"

        register_secret_values(canary)
        try:
            raise ValueError(UnsafePayload())
        except ValueError:
            record = _make_record("error", level=logging.ERROR)
            record.exc_info = sys.exc_info()
        payload = json.loads(JsonFormatter().format(record))
        assert canary not in payload["exc_info"]

    def test_non_secret_output_unchanged(self):
        record = _make_record(
            "LIFECYCLE matched | side=BUY | cond=0x3333",
            type="order_lifecycle",
            status="matched",
            side="BUY",
            conditionId="0x" + "33" * 32,
            asset="0x" + "44" * 32,
            size=136.36,
        )
        payload = json.loads(JsonFormatter().format(record))
        assert payload["message"] == record.getMessage()
        assert payload["size"] == 136.36
        assert payload["status"] == "matched"


class TestAllSinksViaConfigureLogging:
    """The emit-time filter must cover JSONL, console, and stderr fallback."""

    def test_jsonl_sink_redacts_message_args_and_extras(self, tmp_path):
        canary = _canary("sink")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        log.info("signing with %s", canary, extra={"note": canary, "api_key": canary})
        _flush(log)
        rows = _jsonl_rows(tmp_path)
        assert rows
        blob = json.dumps(rows)
        assert canary not in blob
        assert REDACTED_PLACEHOLDER in blob

    def test_console_sink_redacts_default_path(self, tmp_path):
        canary = _canary("console")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console() as buffer:
            log.info("plain line with %s inside", canary, extra={"api_key": canary})
            _flush(log)
        output = buffer.getvalue()
        assert canary not in output
        assert REDACTED_PLACEHOLDER in output

    def test_console_sink_redacts_generic_event_panel(self, tmp_path):
        canary = _canary("panel")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console() as buffer:
            log.info(
                "coordinator_failure",
                extra={"type": "coordinator_failure", "detail": f"ctx {canary}"},
            )
            _flush(log)
        assert canary not in buffer.getvalue()

    def test_console_sink_redacts_exception_traceback(self, tmp_path):
        canary = _canary("trace")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        try:
            raise RuntimeError(f"signing exploded with {canary}")
        except RuntimeError:
            exc_info = sys.exc_info()
        record = _make_record("submit failed", level=logging.ERROR)
        record.exc_info = exc_info
        with _captured_console() as buffer:
            for handler in log.handlers:
                if isinstance(handler, logger_module.RichConsoleHandler):
                    handler.emit(record)
            _flush(log)
        assert canary not in buffer.getvalue()

    def test_stderr_fallback_sink_redacts(self, tmp_path, capsys, monkeypatch):
        canary = _canary("stderr")
        register_secret_values(canary)

        class _FailingFileHandler(logger_module._ManagedRotatingFileHandler):
            def __init__(self, *args, **kwargs):  # pragma: no cover - trivial
                raise OSError("synthetic failure to force stderr fallback")

        monkeypatch.setattr(
            logger_module, "_ManagedRotatingFileHandler", _FailingFileHandler
        )
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        stream_handlers = [
            h for h in log.handlers if isinstance(h, logger_module._ManagedStreamHandler)
        ]
        assert stream_handlers, "stderr fallback handler expected"
        log.info("fallback line with %s", canary, extra={"api_key": canary})
        _flush(log)
        err = capsys.readouterr().err
        assert canary not in err
        parsed = [json.loads(line) for line in err.splitlines() if line.strip()]
        assert parsed
        assert all(canary not in json.dumps(row) for row in parsed)

    def test_filter_installed_on_every_managed_handler(self, tmp_path):
        """Handler-level filters cover child loggers, unlike logger filters."""
        from pmcopy.utils.logger import get_logger

        log = configure_logging(log_dir=tmp_path)
        managed = [h for h in log.handlers if getattr(h, "_pm_bot_managed", False)]
        assert managed
        for handler in managed:
            redaction_filters = [
                f for f in handler.filters if isinstance(f, logger_module._SecretRedactionFilter)
            ]
            assert len(redaction_filters) == 1, handler
        # A child logger's records still pass through the redaction filters.
        child = get_logger("RedactionChildProbe")
        canary = _canary("child")
        register_secret_values(canary)
        with _captured_console() as buffer:
            child.info("child line with %s", canary, extra={"api_key": canary})
            _flush(log)
        assert canary not in buffer.getvalue()
        assert canary not in json.dumps(_jsonl_rows(tmp_path))

    def test_console_handler_defense_without_filter(self):
        """A handler added without the global filter still redacts."""
        canary = _canary("nofilter")
        register_secret_values(canary)
        handler = logger_module.RichConsoleHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        record = _make_record(f"standalone line {canary}", api_key=canary)
        with _captured_console() as buffer:
            handler.emit(record)
        assert canary not in buffer.getvalue()
        assert REDACTED_PLACEHOLDER in buffer.getvalue()


class TestSafeErrorDetail:
    def test_registered_canary_redacted(self):
        canary = _canary("sed")
        register_secret_values(canary)
        out = safe_error_detail(RuntimeError(f"constraint failed after {canary}"))
        assert canary not in out
        assert REDACTED_PLACEHOLDER in out
        assert out.startswith("RuntimeError:")

    def test_pattern_canaries_redacted_without_registry(self):
        bearer = _canary("sedbearer")
        apikey = _canary("sedkey")
        out = safe_error_detail(
            RuntimeError(
                f"GET failed Authorization: Bearer {bearer} url?apikey={apikey}"
            )
        )
        assert bearer not in out
        assert apikey not in out

    def test_truncation_bounds_preserved(self):
        filler = "x" * 400
        out = safe_error_detail(RuntimeError(filler))
        assert out.endswith("…")
        assert len(out) <= 300 + len("RuntimeError: …")

    def test_blank_message_exception_args_repr_redacted(self):
        canary = _canary("blank")

        class BlankError(RuntimeError):
            def __str__(self) -> str:  # pragma: no cover - trivial
                return ""

        register_secret_values(canary)
        out = safe_error_detail(BlankError(canary))
        assert canary not in out

    def test_none_returns_unknown_error(self):
        assert safe_error_detail(None) == "unknown_error"


class TestLogApiHelpers:
    def test_log_api_error_redacts_detail(self, tmp_path):
        canary = _canary("apier")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        log_api_error(
            log,
            side="BUY",
            error=f"PolyApiException[status_code=400, error_message='bad request with {canary}']",
            condition_id="0x" + "33" * 32,
        )
        _flush(log)
        blob = json.dumps(_jsonl_rows(tmp_path))
        assert canary not in blob

    def test_log_api_response_error_detail_redacted(self, tmp_path):
        canary = _canary("resp")
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        log_api_response(
            log,
            method="GET",
            url=f"https://vendor.test/v1/orders?apikey={canary}",
            status=400,
            data={"success": False},
            error=(
                f"PolyApiException[status_code=400, error_message='GET failed "
                f"apikey={canary}']"
            ),
        )
        _flush(log)
        rows = [row for row in _jsonl_rows(tmp_path) if row.get("type") == "api_response"]
        assert rows
        row = rows[-1]
        assert canary not in json.dumps(row)
        assert row.get("error") == "bad_request"
        # The raw error string must not be written verbatim anymore.
        assert row.get("error_detail") != (
            f"PolyApiException[status_code=400, error_message='GET failed apikey={canary}']"
        )
        assert canary not in row.get("url", "")
        assert canary not in row.get("message", "")

    def test_log_api_response_console_render_redacted(self, tmp_path):
        canary = _canary("respc")
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console() as buffer:
            log_api_response(
                log,
                method="GET",
                url=f"https://vendor.test/v1/orders?apikey={canary}",
                status=400,
                error=f"GET failed with apikey={canary}",
            )
            _flush(log)
        assert canary not in buffer.getvalue()


class TestLifecycleDedupAccountScoping:
    CONDITION = "0x" + "33" * 32
    SIGNAL = "signal-shared-0001"

    def _emit(self, log, *, status, account=None, order_id=None, condition_id=None, signal_id=None):
        log_order_lifecycle(
            log,
            status=status,
            side="BUY",
            condition_id=condition_id or self.CONDITION,
            asset="0x" + "44" * 32,
            order_id=order_id,
            size=1.0,
            price=0.5,
            usdc_value=0.5,
            signal_id=signal_id,
            account=account,
        )

    def _rows(self, tmp_path):
        return [row for row in _jsonl_rows(tmp_path) if row.get("type") == "order_lifecycle"]

    def test_two_accounts_same_signal_both_emitted(self, tmp_path):
        _reset_lifecycle_dedup_for_tests()
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console():
            self._emit(log, status="matched", account="user-a", signal_id=self.SIGNAL)
            self._emit(log, status="matched", account="user-b", signal_id=self.SIGNAL)
            _flush(log)
        rows = self._rows(tmp_path)
        assert len(rows) == 2, "two copiers' events on one signal must not collapse"

    def test_same_account_repeat_status_deduped(self, tmp_path):
        _reset_lifecycle_dedup_for_tests()
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console():
            self._emit(log, status="matched", account="user-a", signal_id=self.SIGNAL)
            self._emit(log, status="matched", account="user-a", signal_id=self.SIGNAL)
            _flush(log)
        rows = self._rows(tmp_path)
        assert len(rows) == 1

    def test_account_none_keeps_legacy_dedup(self, tmp_path):
        _reset_lifecycle_dedup_for_tests()
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console():
            self._emit(log, status="matched", signal_id=self.SIGNAL)
            self._emit(log, status="matched", signal_id=self.SIGNAL)
            _flush(log)
        rows = self._rows(tmp_path)
        assert len(rows) == 1

    def test_account_none_payload_has_no_account_field(self, tmp_path):
        _reset_lifecycle_dedup_for_tests()
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console():
            self._emit(log, status="matched", order_id="0x" + "cd" * 32)
            _flush(log)
        row = self._rows(tmp_path)[0]
        assert "account" not in row

    def test_order_id_wins_over_account_scoping(self, tmp_path):
        _reset_lifecycle_dedup_for_tests()
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        order_id = "0x" + "cd" * 32
        with _captured_console():
            self._emit(log, status="matched", account="user-a", order_id=order_id)
            self._emit(log, status="matched", account="user-b", order_id=order_id)
            _flush(log)
        rows = self._rows(tmp_path)
        assert len(rows) == 1, "venue order id is globally unique; dedup stays legacy"

    def test_status_change_still_emits_within_account(self, tmp_path):
        _reset_lifecycle_dedup_for_tests()
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console():
            self._emit(log, status="matched", account="user-a", signal_id=self.SIGNAL)
            self._emit(log, status="confirmed", account="user-a", signal_id=self.SIGNAL)
            _flush(log)
        rows = self._rows(tmp_path)
        assert [row["status"] for row in rows] == ["matched", "confirmed"]


class TestConsoleExtraValueSafety:
    """Console extras render redacted strings even for arbitrary objects."""

    def test_object_extra_str_redacted(self, tmp_path):
        canary = _canary("obj")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console() as buffer:
            log.info("odd extra", extra={"widget": SimpleNamespace(note=canary)})
            _flush(log)
        assert canary not in buffer.getvalue()


class TestPluralCredentialKeys:
    """Follow-up F3-1: plural credential keys classify as secrets.

    ``api_keys``, ``secrets``, ``passwords``, ``keys`` (exact) and sensible
    plurals of the existing singular set must redact — while the market
    identity exemptions (``*_id``/``*_ids``) and the outcome-share
    vocabulary (``tokens``) stay readable. The bare plural ``keys`` is only
    an exact full-key match: longer identifiers merely *containing* ``keys``
    (``hot_keys``, ``keyboard_keys``) must not be caught.
    """

    @pytest.mark.parametrize(
        "key",
        [
            "api_keys",
            "apikeys",
            "ApiKeys",
            "API_KEYS",
            "secrets",
            "SECRETS",
            "passwords",
            "passphrases",
            "keys",
            "Keys",
            "private_keys",
            "priv_keys",
            "client_keys",
            "relayer_keys",
            "api_tokens",
            "access_tokens",
            "refresh_tokens",
            "session_tokens",
            "auth_headers",
            "falcon_api_keys",  # adjacent-segment compound via the pair rule
            "wallet_private_keys",  # strong segment compound
            "db_passwords",
        ],
    )
    def test_plural_credential_keys_are_secret(self, key):
        assert is_secret_key(key) is True

    @pytest.mark.parametrize(
        "key",
        [
            # Market identity exemptions untouched.
            "token_id",
            "token_ids",
            "pruned_token_ids",
            "condition_ids",
            "api_keys_id",
            # Outcome-share vocabulary: "tokens" means shares, never creds.
            "tokens",
            "outcome_tokens",
            "tokens_filled",
            # Bare "keys" must NOT bleed into longer identifiers.
            "hot_keys",
            "keyboard_keys",
            "arrow_keys",
            "leader_keys",
            "signing_keys",
            "monitored_leader_keys",
        ],
    )
    def test_plural_lookalikes_are_not_secret(self, key):
        assert is_secret_key(key) is False

    def test_plural_key_extra_redacted_in_jsonl(self, tmp_path):
        """RED contract: extras={"api_keys": [canary]} must not pass through."""
        canary = _canary("plur")
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        log.info(
            "provider handshake",
            extra={"api_keys": [canary], "secrets": {"vault": canary}},
        )
        _flush(log)
        rows = _jsonl_rows(tmp_path)
        assert rows
        blob = json.dumps(rows)
        assert canary not in blob
        assert rows[-1]["api_keys"] == REDACTED_PLACEHOLDER
        assert rows[-1]["secrets"] == REDACTED_PLACEHOLDER


class TestRedactObjectRobustness:
    """Follow-up F3-2: bounded depth + cycle guard close the fail-open path.

    Before the fix a cyclic or over-deep extra raised RecursionError inside
    the emit-time filter, which swallowed it and let the record continue
    with the RAW (unredacted) structure attached — and dropped/crashed the
    record at the sinks. Cyclic/over-deep containers must instead render a
    safe placeholder.
    """

    DEPTH_PLACEHOLDER = "***REDACTED:depth***"
    CYCLE_PLACEHOLDER = "***REDACTED:cycle***"

    @staticmethod
    def _nested(depth: int, leaf: object) -> dict:
        root: dict = {}
        cursor = root
        for _ in range(depth):
            cursor["n"] = {}
            cursor = cursor["n"]
        cursor["leaf"] = leaf
        return root

    def test_cyclic_dict_redacted_not_raising(self):
        canary = _canary("cyc")
        register_secret_values(canary)
        payload = {"note": f"ctx {canary}", "inner": {}}
        payload["inner"]["back"] = payload
        out = redact_object(payload)  # must not raise RecursionError
        rendered = json.dumps(out)  # result is JSON-serializable
        assert canary not in rendered
        assert self.CYCLE_PLACEHOLDER in rendered

    def test_mutual_dict_list_cycle_redacted(self):
        canary = _canary("mutual")
        register_secret_values(canary)
        holder: list = [f"plain-{canary}"]
        payload = {"items": holder, "note": canary}
        holder.append(payload)
        out = redact_object(payload)
        rendered = json.dumps(out)
        assert canary not in rendered
        assert self.CYCLE_PLACEHOLDER in rendered

    def test_shared_noncyclic_reference_still_rendered(self):
        """A DAG (same object twice, no cycle) keeps full rendering."""
        shared = {"size": 5}
        payload = {"a": shared, "b": shared}
        out = redact_object(payload)
        assert out == {"a": {"size": 5}, "b": {"size": 5}}

    def test_depth_cap_replaces_overdeep_containers(self):
        canary = _canary("deep")
        register_secret_values(canary)
        out = redact_object(self._nested(50, f"leaf-{canary}"))
        rendered = json.dumps(out)
        assert self.DEPTH_PLACEHOLDER in rendered
        assert canary not in rendered

    def test_within_depth_structure_renders_normally(self):
        nested = self._nested(4, "0x" + "44" * 32)
        out = redact_object(nested)
        assert out == nested  # plain market identity untouched, no placeholder
        assert self.DEPTH_PLACEHOLDER not in json.dumps(out)

    def test_shallow_levels_still_redact_in_overdeep_structure(self):
        canary = _canary("mix")
        register_secret_values(canary)
        payload = self._nested(50, "leaf")
        payload["api_key"] = canary  # secret at the TOP level of the same extra
        out = redact_object(payload)
        assert out["api_key"] == REDACTED_PLACEHOLDER

    def test_extreme_depth_does_not_raise(self):
        payload = self._nested(3000, "leaf")
        out = redact_object(payload)  # must not raise RecursionError
        assert self.DEPTH_PLACEHOLDER in json.dumps(out)

    def test_filter_leaves_safe_structure_on_record(self, tmp_path):
        """Emit-time pass must not strand the raw cyclic extra on the record."""
        canary = _canary("filter")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        cyclic = {"note": f"ctx {canary}"}
        cyclic["self"] = cyclic
        log.info("state snapshot", extra={"state": cyclic})
        _flush(log)
        rows = _jsonl_rows(tmp_path)
        assert rows
        state = rows[-1]["state"]
        # Field survives (not silently dropped) and carries no canary.
        blob = json.dumps(state)
        assert canary not in blob
        assert self.CYCLE_PLACEHOLDER in blob

    def test_console_renders_cyclic_extra_without_dying(self, tmp_path):
        canary = _canary("conscyc")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        cyclic = {"note": canary}
        cyclic["self"] = cyclic
        with _captured_console() as buffer:
            log.info("state snapshot", extra={"state": cyclic})
            _flush(log)
        output = buffer.getvalue()
        assert canary not in output
        assert "state snapshot" in output

    def test_json_formatter_defense_handles_cyclic_extra(self):
        """JsonFormatter without the filter must not lose the whole record."""
        canary = _canary("jsncyc")
        register_secret_values(canary)
        record = _make_record("snapshot")
        cyclic = {"note": canary}
        cyclic["self"] = cyclic
        record.state = cyclic
        rendered = JsonFormatter().format(record)  # must not raise
        payload = json.loads(rendered)
        assert payload["message"] == "snapshot"
        assert canary not in rendered
        assert self.CYCLE_PLACEHOLDER in json.dumps(payload.get("state"))


class TestStackInfoRedaction:
    """Follow-up F3-3: the emit-time pass must redact ``stack_info`` text.

    Realism analysis (characterized on Python 3.14): the stdlib
    ``Formatter.format`` unconditionally appends ``formatStack`` output to
    the rendered message, and the captured stack contains frame SOURCE
    LINES plus the (already redacted) message — not frame locals. A real
    canary therefore only reaches the stack text if a credential literal
    is committed to source (forbidden by repository policy) or a caller
    crafts ``record.stack_info`` directly. The render path is still
    defensive in depth today (the console default path wraps the whole
    formatted output in ``redact_text``), BUT the single emit-time pass
    leaves the RAW stack text on the record — any handler attached without
    that wrapper renders it verbatim. The fix closes the asymmetry the
    same way ``exc_text`` was meant to be handled (that branch was dead:
    ``exc_text`` is a standard attribute skipped before the check).
    """

    @staticmethod
    def _record_with_stack(stack_text: str, msg: str = "operation failed") -> logging.LogRecord:
        record = logging.LogRecord(
            name="stack.test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None,
        )
        record.stack_info = stack_text
        return record

    def test_redact_record_redacts_stack_info_in_place(self):
        canary = _canary("stack")
        register_secret_values(canary)
        record = self._record_with_stack(
            f"Stack (most recent call last):\n  File \"signer.py\", line 9\n"
            f"    signed_with = {canary}\n"
        )
        logger_module._redact_record(record)
        assert record.stack_info is not None
        assert canary not in record.stack_info
        assert REDACTED_PLACEHOLDER in record.stack_info

    def test_filter_redacts_stack_info_in_place(self):
        canary = _canary("stackf")
        register_secret_values(canary)
        record = self._record_with_stack(f"Stack (most recent call last):\n    ctx {canary}\n")
        assert logger_module._SecretRedactionFilter().filter(record) is True
        assert canary not in (record.stack_info or "")

    def test_redact_record_redacts_cached_exc_text_in_place(self):
        """The pre-existing ``exc_text`` branch was unreachable; repair it."""
        canary = _canary("exctext")
        register_secret_values(canary)
        record = _make_record("failed", level=logging.ERROR)
        record.exc_text = f"RuntimeError: signing exploded with {canary}\n"
        logger_module._redact_record(record)
        assert canary not in (record.exc_text or "")

    def test_console_default_path_renders_redacted_stack(self, tmp_path):
        """Regression guard: console render never shows raw stack text."""
        canary = _canary("stackc")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        record = self._record_with_stack(f"Stack (most recent call last):\n    ctx {canary}\n")
        with _captured_console() as buffer:
            log.handle(record)
            _flush(log)
        assert canary not in buffer.getvalue()

    def test_json_formatter_still_ignores_stack_info(self):
        """JsonFormatter must keep ignoring stack_info entirely."""
        canary = _canary("stackj")
        register_secret_values(canary)
        record = self._record_with_stack(f"Stack (most recent call last):\n    ctx {canary}\n")
        payload = json.loads(JsonFormatter().format(record))
        assert "stack_info" not in payload
        assert canary not in payload["message"]

    def test_real_stack_info_call_never_leaks_console(self, tmp_path):
        """End-to-end: an ordinary ``stack_info=True`` error stays clean."""
        canary = _canary("stackreal")
        register_secret_values(canary)
        log = configure_logging(log_dir=tmp_path, level="DEBUG")
        with _captured_console() as buffer:
            log.error("signing failed near %s", canary, stack_info=True)
            _flush(log)
        assert canary not in buffer.getvalue()
        assert "Stack (most recent call last)" in buffer.getvalue()
