from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


CanonicalExecutionMode = Literal["canonical", "raw_sheet"]
PlanScope = Literal["all_sheets", "selected_sheets"]
FilterOperator = Literal[
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "contains",
    "in",
    "between",
    "is_null",
    "not_null",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OfficeRecord(StrictModel):
    office_code: str
    sheet_name: str
    office_name: str
    division: str
    district: str
    address: str = ""
    email: str = ""
    office_time: str = ""
    source_url: str
    serial_number: int


class OfficeManifest(StrictModel):
    generated_at: datetime
    retrieved_on: str
    office_count: int
    source_urls: dict[str, str]
    offices: list[OfficeRecord]


class ColumnProfile(StrictModel):
    original_name: str
    sanitized_name: str
    inferred_dtype: str
    null_fraction: float
    sample_values: list[str] = Field(default_factory=list)


class MappingDecision(StrictModel):
    canonical_field: str | None = None
    source_column: str
    confidence: float = 0.0
    strategy: str
    notes: str = ""


class SheetProfile(StrictModel):
    sheet_name: str
    raw_table_name: str
    row_count: int
    column_count: int
    column_profiles: list[ColumnProfile] = Field(default_factory=list)
    mapping_decisions: list[MappingDecision] = Field(default_factory=list)
    mapped_canonical_fields: list[str] = Field(default_factory=list)
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)


class WorkbookProfile(StrictModel):
    ingestion_run_id: str
    workbook_name: str
    workbook_path: str
    created_at: datetime
    total_rows: int
    total_sheets: int
    sheet_profiles: list[SheetProfile] = Field(default_factory=list)


class PlanFilter(StrictModel):
    field: str
    operator: FilterOperator
    value: str | list[str] | None = None
    rationale: str = ""

    @field_validator("value", mode="before")
    @classmethod
    def coerce_value(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, list):
            return [str(item) for item in value]
        return str(value)


class PlanSort(StrictModel):
    field: str
    direction: Literal["asc", "desc"] = "asc"


class QueryPlan(StrictModel):
    goal: str
    target_scope: PlanScope = "all_sheets"
    target_sheets: list[str] = Field(default_factory=list)
    execution_mode: CanonicalExecutionMode = "canonical"
    selected_columns: list[str] = Field(default_factory=list)
    filters: list[PlanFilter] = Field(default_factory=list)
    sort: list[PlanSort] = Field(default_factory=list)
    limit: int = 200
    assumptions: list[str] = Field(default_factory=list)
    ambiguities: list[str] = Field(default_factory=list)
    needs_user_confirmation: bool = True
    explanation: str = ""


class SchemaMappingSuggestion(StrictModel):
    source_column: str
    canonical_field: str | None = None
    confidence: float
    reasoning: str = ""


class SchemaMappingResponse(StrictModel):
    suggestions: list[SchemaMappingSuggestion] = Field(default_factory=list)


class SyntheticSheetMetadata(StrictModel):
    office_code: str
    office_name: str
    division: str
    district: str
    sheet_name: str
    schema_family: str
    row_count: int
    canonical_map: dict[str, str]


class SyntheticWorkbookMetadata(StrictModel):
    generated_at: datetime
    total_rows: int
    total_offices: int
    workbook_path: str
    manifest_path: str
    sheets: list[SyntheticSheetMetadata] = Field(default_factory=list)
