from __future__ import annotations

import shutil
import time
from pathlib import Path


class RuntimeEnvEditor:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def read_leaders(self) -> list[str]:
        if not self.path.exists():
            return []
        lines = self.path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            if line.strip().startswith("LEADER_WALLETS="):
                raw = line.split("=", 1)[1]
                return [x.strip().lower() for x in raw.split(",") if x.strip()]
        return []

    def write_leaders(self, leaders: list[str]) -> Path:
        backup = self.path.with_name(f"{self.path.name}.bak.{int(time.time())}")
        shutil.copy2(self.path, backup)

        lines = self.path.read_text(encoding="utf-8").splitlines()
        new_line = "LEADER_WALLETS=" + ",".join(leaders)
        replaced = False
        out: list[str] = []
        for line in lines:
            if line.strip().startswith("LEADER_WALLETS="):
                out.append(new_line)
                replaced = True
            else:
                out.append(line)
        if not replaced:
            out.append(new_line)

        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
        tmp.replace(self.path)
        return backup

