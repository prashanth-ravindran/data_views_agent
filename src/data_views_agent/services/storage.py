from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from data_views_agent.models.contracts import SheetProfile
from data_views_agent.utils.text import ascii_slug, json_dumps


CANONICAL_TABLE = "canonical_registrations"


class SQLiteStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA foreign_keys=ON;")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    ingestion_run_id TEXT PRIMARY KEY,
                    workbook_name TEXT NOT NULL,
                    workbook_path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    total_sheets INTEGER NOT NULL DEFAULT 0,
                    total_rows INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS sheet_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingestion_run_id TEXT NOT NULL,
                    sheet_name TEXT NOT NULL,
                    raw_table_name TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    column_count INTEGER NOT NULL,
                    mapped_canonical_fields_json TEXT NOT NULL,
                    sample_rows_json TEXT NOT NULL,
                    profile_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS column_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingestion_run_id TEXT NOT NULL,
                    sheet_name TEXT NOT NULL,
                    original_name TEXT NOT NULL,
                    sanitized_name TEXT NOT NULL,
                    inferred_dtype TEXT NOT NULL,
                    null_fraction REAL NOT NULL,
                    sample_values_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS column_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingestion_run_id TEXT NOT NULL,
                    sheet_name TEXT NOT NULL,
                    canonical_field TEXT,
                    source_column TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    strategy TEXT NOT NULL,
                    notes TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS query_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingestion_run_id TEXT NOT NULL,
                    user_prompt TEXT NOT NULL,
                    generated_plan_json TEXT NOT NULL,
                    approved_plan_json TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    sql_params_json TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS {CANONICAL_TABLE} (
                    ingestion_run_id TEXT NOT NULL,
                    source_sheet TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    source_row_id INTEGER NOT NULL,
                    office_code TEXT,
                    office_name TEXT,
                    division TEXT,
                    district TEXT,
                    registration_date TEXT,
                    document_number TEXT,
                    document_type TEXT,
                    property_type TEXT,
                    property_usage TEXT,
                    locality TEXT,
                    village TEXT,
                    survey_number TEXT,
                    project_name TEXT,
                    buyer_name TEXT,
                    seller_name TEXT,
                    consideration_value REAL,
                    market_value REAL,
                    stamp_duty REAL,
                    registration_fee REAL,
                    area_sqft REAL,
                    status TEXT,
                    raw_payload_json TEXT
                );
                """
            )

    def create_ingestion_run(self, workbook_name: str, workbook_path: str) -> str:
        ingestion_run_id = uuid.uuid4().hex
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs (ingestion_run_id, workbook_name, workbook_path, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (ingestion_run_id, workbook_name, workbook_path, datetime.now(UTC).isoformat()),
            )
        return ingestion_run_id

    def finalize_ingestion_run(self, ingestion_run_id: str, total_sheets: int, total_rows: int) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET total_sheets = ?, total_rows = ?
                WHERE ingestion_run_id = ?
                """,
                (total_sheets, total_rows, ingestion_run_id),
            )

    def raw_table_name_for(self, ingestion_run_id: str, sheet_name: str) -> str:
        return f"raw_{ingestion_run_id[:8]}_{ascii_slug(sheet_name, default='sheet', max_length=28)}"

    def write_raw_sheet(self, ingestion_run_id: str, sheet_name: str, dataframe: pd.DataFrame) -> str:
        table_name = self.raw_table_name_for(ingestion_run_id, sheet_name)
        enriched = dataframe.copy()
        enriched.insert(0, "source_row_id", range(1, len(enriched.index) + 1))
        enriched.insert(0, "ingestion_run_id", ingestion_run_id)
        with self.connect() as connection:
            enriched.to_sql(table_name, connection, if_exists="replace", index=False)
        return table_name

    def append_canonical_rows(self, dataframe: pd.DataFrame) -> None:
        with self.connect() as connection:
            dataframe.to_sql(CANONICAL_TABLE, connection, if_exists="append", index=False)

    def save_sheet_profile(self, ingestion_run_id: str, profile: SheetProfile) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO sheet_profiles (
                    ingestion_run_id, sheet_name, raw_table_name, row_count, column_count,
                    mapped_canonical_fields_json, sample_rows_json, profile_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingestion_run_id,
                    profile.sheet_name,
                    profile.raw_table_name,
                    profile.row_count,
                    profile.column_count,
                    json_dumps(profile.mapped_canonical_fields),
                    json_dumps(profile.sample_rows),
                    json_dumps(profile.model_dump(mode="json")),
                ),
            )

            connection.executemany(
                """
                INSERT INTO column_profiles (
                    ingestion_run_id, sheet_name, original_name, sanitized_name, inferred_dtype,
                    null_fraction, sample_values_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ingestion_run_id,
                        profile.sheet_name,
                        column.original_name,
                        column.sanitized_name,
                        column.inferred_dtype,
                        column.null_fraction,
                        json_dumps(column.sample_values),
                    )
                    for column in profile.column_profiles
                ],
            )

            connection.executemany(
                """
                INSERT INTO column_mappings (
                    ingestion_run_id, sheet_name, canonical_field, source_column, confidence, strategy, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ingestion_run_id,
                        profile.sheet_name,
                        decision.canonical_field,
                        decision.source_column,
                        decision.confidence,
                        decision.strategy,
                        decision.notes,
                    )
                    for decision in profile.mapping_decisions
                ],
            )

    def get_table_columns(self, table_name: str) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        return [row["name"] for row in rows]

    def fetch_sheet_catalog(self, ingestion_run_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT sheet_name, raw_table_name, row_count, column_count, mapped_canonical_fields_json
                FROM sheet_profiles
                WHERE ingestion_run_id = ?
                ORDER BY sheet_name
                """,
                (ingestion_run_id,),
            ).fetchall()
        return [
            {
                "sheet_name": row["sheet_name"],
                "raw_table_name": row["raw_table_name"],
                "row_count": row["row_count"],
                "column_count": row["column_count"],
                "mapped_canonical_fields": json.loads(row["mapped_canonical_fields_json"]),
            }
            for row in rows
        ]

    def fetch_sheet_columns(self, ingestion_run_id: str, sheet_names: Iterable[str]) -> dict[str, list[str]]:
        sheet_names = list(sheet_names)
        if not sheet_names:
            return {}
        placeholders = ",".join("?" for _ in sheet_names)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT sheet_name, sanitized_name
                FROM column_profiles
                WHERE ingestion_run_id = ? AND sheet_name IN ({placeholders})
                ORDER BY sheet_name, sanitized_name
                """,
                (ingestion_run_id, *sheet_names),
            ).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            result.setdefault(row["sheet_name"], []).append(row["sanitized_name"])
        return result

    def fetch_distinct_values(self, ingestion_run_id: str, field_name: str, limit: int = 25) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT DISTINCT "{field_name}" AS value
                FROM {CANONICAL_TABLE}
                WHERE ingestion_run_id = ? AND "{field_name}" IS NOT NULL AND TRIM("{field_name}") != ''
                ORDER BY value
                LIMIT ?
                """,
                (ingestion_run_id, limit),
            ).fetchall()
        return [row["value"] for row in rows if row["value"] is not None]

    def current_run_summary(self, ingestion_run_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT ingestion_run_id, workbook_name, workbook_path, created_at, total_sheets, total_rows
                FROM ingestion_runs
                WHERE ingestion_run_id = ?
                """,
                (ingestion_run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown ingestion run: {ingestion_run_id}")
        return dict(row)

    def execute_query(self, sql_text: str, params: list[Any] | tuple[Any, ...]) -> pd.DataFrame:
        with self.connect() as connection:
            return pd.read_sql_query(sql_text, connection, params=params)

    def count_query(self, sql_text: str, params: list[Any] | tuple[Any, ...]) -> int:
        with self.connect() as connection:
            row = connection.execute(sql_text, params).fetchone()
        return int(row[0]) if row else 0

    def log_query(
        self,
        ingestion_run_id: str,
        user_prompt: str,
        generated_plan_json: str,
        approved_plan_json: str,
        sql_text: str,
        sql_params: list[Any],
        row_count: int,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO query_logs (
                    ingestion_run_id, user_prompt, generated_plan_json, approved_plan_json,
                    sql_text, sql_params_json, row_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingestion_run_id,
                    user_prompt,
                    generated_plan_json,
                    approved_plan_json,
                    sql_text,
                    json.dumps(sql_params, ensure_ascii=False),
                    row_count,
                    datetime.now(UTC).isoformat(),
                ),
            )

    def fetch_recent_query_logs(self, ingestion_run_id: str, limit: int = 10) -> pd.DataFrame:
        with self.connect() as connection:
            return pd.read_sql_query(
                """
                SELECT
                    created_at AS run_time,
                    user_prompt AS request,
                    row_count AS matches
                FROM query_logs
                WHERE ingestion_run_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                connection,
                params=(ingestion_run_id, limit),
            )
