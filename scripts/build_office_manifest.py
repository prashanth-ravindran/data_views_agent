from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_views_agent.config import get_settings
from data_views_agent.services.office_manifest import write_office_manifest


def main() -> None:
    settings = get_settings()
    manifest = write_office_manifest(settings.office_manifest_path)
    print(f"Wrote {manifest.office_count} office records to {settings.office_manifest_path}")


if __name__ == "__main__":
    main()

