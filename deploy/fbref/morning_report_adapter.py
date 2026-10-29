"""Future host integration; importing this module performs no I/O."""

import os
from pathlib import Path
import subprocess


def fbref_daily_report() -> list[str]:
    root = Path(os.environ.get("FBREF_DAILY_REPORT_ROOT", "/root/data-platform-football/.local/releases/fbref-1323"))
    output = Path("/root/data-platform-football/.local/reports/fbref/daily")
    script = root / "scripts/report_fbref_daily.py"
    if not script.is_file():
        return ["• FBref #1323: измеритель ещё не доставлен; приёмка не подтверждена ⚠️"]
    try:
        result = subprocess.run([
            os.environ.get("FBREF_REPORT_PYTHON", "/usr/bin/python3"), "-B", str(script),
            "--host-read-only", "--output-dir", str(output),
        ], capture_output=True, text=True, timeout=180, check=True)
        lines = result.stdout.strip().splitlines()
        if (len(lines) < 4 or not lines[0].startswith("FBref: матчей ")
                or not lines[3].startswith("Возраст по видам страниц: ")):
            raise ValueError("Unexpected report output")
        if len(lines) > 4 and (
                len(lines) < 9 or not lines[4].startswith("FBref: недельная полнота ")
                or lines[5] != "| Турнир | Период UTC | FotMob N | FBref-Bronze M | M−N | Доказательства; сопоставление |"
                or lines[6] != "| --- | --- | ---: | ---: | ---: | --- |"
                or any(not line.startswith("| ") or not line.endswith(" |") for line in lines[7:-2])
                or not lines[-2].startswith("Без соответствия FotMob: ")
                or not lines[-1].startswith("Ограничения недели: ")):
            raise ValueError("Unexpected weekly report output")
        return ["• " + line for line in lines]
    except (OSError, subprocess.SubprocessError, ValueError):
        return ["• FBref #1323: отчёт недоступен; приёмка не подтверждена ⚠️"]
