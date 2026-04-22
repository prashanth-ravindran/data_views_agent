from pathlib import Path

import pandas as pd

from data_views_agent.models.contracts import OfficeManifest, OfficeRecord
from data_views_agent.services.synthetic_data import generate_workbook


def test_generate_workbook_creates_expected_sheets(tmp_path: Path) -> None:
    manifest = OfficeManifest(
        generated_at="2026-04-22T00:00:00",
        retrieved_on="2026-04-22",
        office_count=2,
        source_urls={"demo": "https://example.com"},
        offices=[
            OfficeRecord(
                office_code="pune-001",
                sheet_name="PUN_001_haveli_1",
                office_name="Jt Sub Registrar Haveli 1",
                division="Pune",
                district="Pune",
                address="Pune",
                email="demo1@example.com",
                office_time="10 to 5",
                source_url="https://example.com",
                serial_number=1,
            ),
            OfficeRecord(
                office_code="nagpur-002",
                sheet_name="NAG_002_nagpur_city_2",
                office_name="Jt Sub Registrar Nagpur City 2",
                division="Nagpur",
                district="Nagpur",
                address="Nagpur",
                email="demo2@example.com",
                office_time="10 to 5",
                source_url="https://example.com",
                serial_number=2,
            ),
        ],
    )
    output_path = tmp_path / "demo.xlsx"
    metadata_path = tmp_path / "demo.metadata.json"

    metadata = generate_workbook(manifest, output_path, metadata_path, total_rows=100, seed=11)
    workbook = pd.ExcelFile(output_path)

    assert metadata.total_offices == 2
    assert set(workbook.sheet_names) == {"PUN_001_haveli_1", "NAG_002_nagpur_city_2"}
    assert metadata_path.exists()

