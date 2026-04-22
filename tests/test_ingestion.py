from pathlib import Path

import pandas as pd

from data_views_agent.services.ingestion import WorkbookIngestionService
from data_views_agent.services.storage import SQLiteStore


def test_ingestion_normalizes_heterogeneous_sheets(tmp_path: Path) -> None:
    workbook_path = tmp_path / "heterogeneous.xlsx"
    db_path = tmp_path / "test.sqlite"

    with pd.ExcelWriter(workbook_path, engine="xlsxwriter") as writer:
        pd.DataFrame(
            [
                {
                    "office_name": "Pune Office 1",
                    "district": "Pune",
                    "registration_date": "2025-01-01",
                    "buyer_name": "Aarav Patil",
                    "seller_name": "Neha Joshi",
                    "market_value": 15000000,
                }
            ]
        ).to_excel(writer, sheet_name="SheetA", index=False)
        pd.DataFrame(
            [
                {
                    "sub_registrar_office": "Nagpur Office 2",
                    "district_name": "Nagpur",
                    "exec_date": "2025-02-01",
                    "purchaser": "Omkar Naik",
                    "vendor": "Aditi More",
                    "ready_reckoner_value": 9200000,
                }
            ]
        ).to_excel(writer, sheet_name="SheetB", index=False)

    store = SQLiteStore(db_path)
    ingestor = WorkbookIngestionService(store)
    profile = ingestor.ingest_workbook(workbook_path)

    result = store.execute_query(
        """
        SELECT office_name, district, registration_date, buyer_name, seller_name, market_value
        FROM canonical_registrations
        WHERE ingestion_run_id = ?
        ORDER BY office_name
        """,
        [profile.ingestion_run_id],
    )

    assert profile.total_sheets == 2
    assert len(result.index) == 2
    assert set(result["district"]) == {"Pune", "Nagpur"}
