from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from data_views_agent.models.contracts import WorkbookProfile
from data_views_agent.services.profiling import AREA_FACTORS, DATE_FIELDS, NUMERIC_FIELDS, best_mapping_by_canonical, profile_sheet, sanitize_dataframe_headers
from data_views_agent.services.storage import SQLiteStore
from data_views_agent.utils.text import compact_whitespace, json_dumps, jsonable_value, maybe_date_string, maybe_float


def _normalize_area(source_column: str, numeric_value: float | None) -> float | None:
    if numeric_value is None:
        return None
    source = source_column.lower()
    for key, factor in AREA_FACTORS.items():
        if key in source:
            return round(numeric_value * factor, 2)
    return round(numeric_value, 2)


def _normalize_scalar(field_name: str, source_column: str, value: Any) -> Any:
    if field_name in DATE_FIELDS:
        return maybe_date_string(value)
    if field_name in NUMERIC_FIELDS:
        numeric_value = maybe_float(value)
        if field_name == "area_sqft":
            return _normalize_area(source_column, numeric_value)
        return round(numeric_value, 2) if numeric_value is not None else None

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return compact_whitespace(str(value))


def build_canonical_dataframe(
    ingestion_run_id: str,
    sheet_name: str,
    raw_table_name: str,
    sanitized_df: pd.DataFrame,
    best_mappings: dict[str, Any],
) -> pd.DataFrame:
    records = []
    rows = sanitized_df.where(pd.notna(sanitized_df), None).to_dict(orient="records")

    for source_row_id, row in enumerate(rows, start=1):
        canonical = {
            "ingestion_run_id": ingestion_run_id,
            "source_sheet": sheet_name,
            "source_table": raw_table_name,
            "source_row_id": source_row_id,
            "office_code": None,
            "office_name": None,
            "division": None,
            "district": None,
            "registration_date": None,
            "document_number": None,
            "document_type": None,
            "property_type": None,
            "property_usage": None,
            "locality": None,
            "village": None,
            "survey_number": None,
            "project_name": None,
            "buyer_name": None,
            "seller_name": None,
            "consideration_value": None,
            "market_value": None,
            "stamp_duty": None,
            "registration_fee": None,
            "area_sqft": None,
            "status": None,
            "raw_payload_json": json_dumps({column: jsonable_value(value) for column, value in row.items()}),
        }

        for field_name, decision in best_mappings.items():
            if decision.source_column not in row:
                continue
            canonical[field_name] = _normalize_scalar(field_name, decision.source_column, row[decision.source_column])

        if not canonical["office_name"]:
            canonical["office_name"] = sheet_name
        records.append(canonical)

    return pd.DataFrame(records)


class WorkbookIngestionService:
    def __init__(self, store: SQLiteStore, schema_mapper: Any | None = None) -> None:
        self.store = store
        self.schema_mapper = schema_mapper

    def ingest_workbook(self, workbook_path: Path) -> WorkbookProfile:
        workbook_path = workbook_path.resolve()
        ingestion_run_id = self.store.create_ingestion_run(workbook_path.name, str(workbook_path))
        excel_file = pd.ExcelFile(workbook_path, engine="openpyxl")

        sheet_profiles = []
        total_rows = 0

        for sheet_name in excel_file.sheet_names:
            dataframe = excel_file.parse(sheet_name=sheet_name)
            sanitized_df, original_to_sanitized = sanitize_dataframe_headers(dataframe)
            raw_table_name = self.store.write_raw_sheet(ingestion_run_id, sheet_name, sanitized_df)
            profile = profile_sheet(
                sheet_name=sheet_name,
                raw_table_name=raw_table_name,
                sanitized_df=sanitized_df,
                original_to_sanitized=original_to_sanitized,
                schema_mapper=self.schema_mapper,
            )
            self.store.save_sheet_profile(ingestion_run_id, profile)

            canonical_df = build_canonical_dataframe(
                ingestion_run_id=ingestion_run_id,
                sheet_name=sheet_name,
                raw_table_name=raw_table_name,
                sanitized_df=sanitized_df,
                best_mappings=best_mapping_by_canonical(profile),
            )
            self.store.append_canonical_rows(canonical_df)

            total_rows += len(sanitized_df.index)
            sheet_profiles.append(profile)

        self.store.finalize_ingestion_run(ingestion_run_id, len(sheet_profiles), total_rows)

        return WorkbookProfile(
            ingestion_run_id=ingestion_run_id,
            workbook_name=workbook_path.name,
            workbook_path=str(workbook_path),
            created_at=datetime.now(UTC),
            total_rows=total_rows,
            total_sheets=len(sheet_profiles),
            sheet_profiles=sheet_profiles,
        )
