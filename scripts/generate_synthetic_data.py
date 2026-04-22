from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_views_agent.config import get_settings
from data_views_agent.services.office_manifest import ensure_office_manifest
from data_views_agent.services.synthetic_data import generate_workbook


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Maharashtra registration Excel data.")
    parser.add_argument("--output", type=Path, required=True, help="Output XLSX path.")
    parser.add_argument("--rows", type=int, default=300000, help="Total rows across all sheets.")
    parser.add_argument("--seed", type=int, default=7, help="Random seed.")
    parser.add_argument("--max-offices", type=int, default=None, help="Optional office-count cap for smaller demo workbooks.")
    parser.add_argument(
        "--include-admin-offices",
        action="store_true",
        help="Include DIG/JDR administrative offices in addition to transaction offices.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    manifest = ensure_office_manifest(settings.office_manifest_path)
    metadata_path = args.output.with_suffix(".metadata.json")
    metadata = generate_workbook(
        manifest,
        args.output,
        metadata_path,
        total_rows=args.rows,
        seed=args.seed,
        max_offices=args.max_offices,
        include_admin_offices=args.include_admin_offices,
    )
    print(f"Workbook written to {args.output}")
    print(f"Metadata written to {metadata_path}")
    print(f"Rows: {metadata.total_rows} | Sheets: {metadata.total_offices}")


if __name__ == "__main__":
    main()
