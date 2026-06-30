"""Dedicated atomic env-patch writer for ``config/runtime.local.env`` (PR3).

Applies a list of :class:`EnvPatch` proposals to the operator's local env
file. Never mutates protected keys (secrets, credentials, RPC, leaders).
Always writes to ``runtime.local.env`` (never the tracked ``runtime.env`` or
the example). Atomic: copies a ``.bak.<ts>`` backup, writes to a ``.tmp``,
then ``os.replace``.

Dry-run by default: ``apply=False`` (the default) computes the patch table
and prints the explanation but writes nothing. ``apply=True`` is required to
mutate the file.

Safety rules (per the deep-research prompt):
  * Never modify secrets (PRIVATE_KEY, CLOB_*, RPC_*, FUNDER_ADDRESS, etc.).
  * Never auto-change LEADER_WALLETS unless explicitly requested.
  * Write only to ``runtime.local.env``.
  * Default to dry-run; ``apply`` required for mutation.
  * Emit a human-readable patch explanation.
  * Never log secret values; protected-key ``old_value`` is redacted.
"""
from __future__ import annotations

import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from models.env_patch import EnvPatch

# Keys the writer will NEVER mutate. The tuner never produces patches for
# these, but the writer re-checks defensively in case a caller bypasses the
# tuner.
_PROTECTED_EXACT = {
    "PRIVATE_KEY",
    "FUNDER_ADDRESS",
    "RPC_URL",
    "RPC_FALLBACK_URLS",
    "CLOB_API_KEY",
    "CLOB_SECRET",
    "CLOB_PASSPHRASE",
    "LEADER_WALLETS",
    "USER_ADDRESS",
    "DATABASE_URL",
    "SIGNATURE_TYPE",
    "CHAIN_ID",
}

# Substrings that mark a key as secret-ish even if not in the exact set.
_PROTECTED_SUBSTRINGS = (
    "SECRET",
    "PRIVATE",
    "PASSWORD",
    "PASSPHRASE",
    "API_KEY",
    "TOKEN",
)


def is_protected_key(key: str) -> bool:
    upper = (key or "").upper().strip()
    if not upper:
        return True
    if upper in _PROTECTED_EXACT:
        return True
    return any(sub in upper for sub in _PROTECTED_SUBSTRINGS)


_KEY_LINE = re.compile(r"^([A-Z][A-Z0-9_]*)\s*=")


@dataclass(frozen=True)
class PatchApplication:
    """Result of applying (or dry-running) a patch set."""

    applied: bool
    target: str
    patches: tuple[EnvPatch, ...]
    rejected: tuple[EnvPatch, ...]
    backup_path: str | None
    explanation: str


class EnvPatchWriter:
    """Apply :class:`EnvPatch` proposals to ``runtime.local.env``."""

    def __init__(self, target: str | Path) -> None:
        self._target = Path(target)

    @property
    def target(self) -> Path:
        return self._target

    def plan(
        self,
        patches: list[EnvPatch],
        *,
        allow_leader_rewrite: bool = False,
    ) -> tuple[list[EnvPatch], list[EnvPatch]]:
        """Split ``patches`` into ``(accepted, rejected)``.

        Protected keys are rejected unless ``allow_leader_rewrite`` is True AND
        the key is ``LEADER_WALLETS`` (the only protected key that may be
        explicitly rewritten).
        """
        accepted: list[EnvPatch] = []
        rejected: list[EnvPatch] = []
        for patch in patches:
            if is_protected_key(patch.key):
                if allow_leader_rewrite and patch.key.upper() == "LEADER_WALLETS":
                    accepted.append(patch)
                else:
                    rejected.append(patch)
                continue
            accepted.append(patch)
        return accepted, rejected

    def apply(
        self,
        patches: list[EnvPatch],
        *,
        apply: bool = False,
        allow_leader_rewrite: bool = False,
    ) -> PatchApplication:
        """Apply (or dry-run) ``patches`` to the target file."""
        accepted, rejected = self.plan(patches, allow_leader_rewrite=allow_leader_rewrite)

        # Read current values (without exposing secrets).
        current_values = self._read_current_values()
        enriched_accepted: list[EnvPatch] = []
        for patch in accepted:
            old = current_values.get(patch.key)
            # Never leak the old value of a protected key — but plan() already
            # rejected those, so this is belt-and-braces.
            old_safe = "<redacted>" if is_protected_key(patch.key) else old
            enriched_accepted.append(
                EnvPatch(
                    key=patch.key,
                    old_value=old_safe,
                    new_value=patch.new_value,
                    reason=patch.reason,
                    confidence=patch.confidence,
                )
            )

        explanation = self._render_explanation(enriched_accepted, rejected, apply)

        backup_path: str | None = None
        if apply and enriched_accepted:
            if not self._target.exists():
                # Create an empty file so the write has something to back up.
                self._target.write_text("", encoding="utf-8")
            backup_path = self._atomic_write(enriched_accepted)

        return PatchApplication(
            applied=apply and bool(enriched_accepted),
            target=str(self._target),
            patches=tuple(enriched_accepted),
            rejected=tuple(rejected),
            backup_path=backup_path,
            explanation=explanation,
        )

    def _read_current_values(self) -> dict[str, str]:
        if not self._target.exists():
            return {}
        values: dict[str, str] = {}
        for line in self._target.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            match = _KEY_LINE.match(stripped)
            if not match:
                continue
            key = match.group(1)
            raw = stripped.split("=", 1)[1] if "=" in stripped else ""
            # Strip inline comments + surrounding whitespace from the value.
            value = raw.split("#", 1)[0].strip()
            values[key] = value
        return values

    def _atomic_write(self, patches: list[EnvPatch]) -> str:
        backup = self._target.with_name(
            f"{self._target.name}.bak.{int(time.time())}"
        )
        shutil.copy2(self._target, backup)

        existing_lines = self._target.read_text(encoding="utf-8").splitlines()
        patch_by_key = {p.key: p for p in patches}
        out: list[str] = []
        replaced: set[str] = set()
        for line in existing_lines:
            stripped = line.strip()
            match = _KEY_LINE.match(stripped)
            if match and match.group(1) in patch_by_key:
                key = match.group(1)
                out.append(f"{key}={patch_by_key[key].new_value}")
                replaced.add(key)
            else:
                out.append(line)
        # Append any new keys not already present.
        for key, patch in patch_by_key.items():
            if key not in replaced:
                out.append(f"{key}={patch.new_value}")

        tmp = self._target.with_suffix(self._target.suffix + ".tmp")
        tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
        os.replace(tmp, self._target)
        return str(backup)

    def _render_explanation(
        self,
        accepted: list[EnvPatch],
        rejected: list[EnvPatch],
        apply: bool,
    ) -> str:
        mode = "APPLY" if apply else "DRY-RUN"
        lines = [f"env patch {mode} -> {self._target}", ""]
        if not accepted and not rejected:
            lines.append("(no patches proposed)")
            return "\n".join(lines)
        if accepted:
            lines.append("accepted:")
            for p in accepted:
                old = p.old_value if p.old_value is not None else "(new)"
                lines.append(
                    f"  {p.key}: {old} -> {p.new_value}  "
                    f"[conf={p.confidence:.2f}]  # {p.reason}"
                )
        if rejected:
            lines.append("")
            lines.append("rejected (protected key):")
            for p in rejected:
                lines.append(f"  {p.key}: refused (would mutate a protected key)")
        return "\n".join(lines)