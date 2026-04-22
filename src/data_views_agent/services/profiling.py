from __future__ import annotations

from collections import defaultdict
import json
import logging
from typing import Any

import pandas as pd

from data_views_agent.models.contracts import (
    ColumnProfile,
    MappingDecision,
    SchemaMappingResponse,
    SheetProfile,
)
from data_views_agent.utils.text import ascii_slug, compact_whitespace, jsonable_value, maybe_date_string

try:
    from google import genai
    from google.genai import types
except Exception:  # pragma: no cover - import fallback for environments without the SDK
    genai = None
    types = None


CANONICAL_FIELD_SPECS: dict[str, dict[str, Any]] = {
    "office_code": {
        "description": "Unique office identifier or short office code.",
        "aliases": ["office_code", "sr_office_code", "officeid", "office_id"],
    },
    "office_name": {
        "description": "Sub-registrar or registration office name.",
        "aliases": ["office_name", "sub_registrar_office", "office_label", "office", "sr_office"],
    },
    "division": {
        "description": "Administrative division or region.",
        "aliases": ["division", "region"],
    },
    "district": {
        "description": "District name.",
        "aliases": ["district", "dist", "registration_district"],
    },
    "registration_date": {
        "description": "Document registration or execution date.",
        "aliases": ["registration_date", "exec_date", "document_date", "reg_date", "date_of_registration"],
    },
    "document_number": {
        "description": "Document, deed, token, or registration number.",
        "aliases": ["document_number", "doc_no", "deed_no", "token_no", "registration_no", "document_no"],
    },
    "document_type": {
        "description": "Type of registered document or deed.",
        "aliases": ["document_type", "doc_type", "deed_type", "article_type", "instrument_type"],
    },
    "property_type": {
        "description": "Property class such as flat, land, shop, plot, industrial unit.",
        "aliases": ["property_type", "prop_type", "asset_type", "unit_type"],
    },
    "property_usage": {
        "description": "Usage such as residential, commercial, industrial, agricultural.",
        "aliases": ["property_usage", "land_use", "usage_type", "property_use", "use_type"],
    },
    "locality": {
        "description": "City area, locality, ward, or neighborhood.",
        "aliases": ["locality", "area", "neighborhood", "zone", "ward", "suburb"],
    },
    "village": {
        "description": "Village or mouza name.",
        "aliases": ["village", "village_name", "mouza", "gaon"],
    },
    "survey_number": {
        "description": "Survey, CTS, gut, plot, or property reference identifier.",
        "aliases": ["survey_number", "survey_no", "cts_no", "gut_no", "plot_no", "property_ref"],
    },
    "project_name": {
        "description": "Builder or project name.",
        "aliases": ["project_name", "builder_project", "scheme_name", "project"],
    },
    "buyer_name": {
        "description": "Primary purchaser or buyer name.",
        "aliases": ["buyer_name", "purchaser", "purchaser_name", "party1", "buyer", "claimant_name"],
    },
    "seller_name": {
        "description": "Primary seller or transferor name.",
        "aliases": ["seller_name", "vendor", "vendor_name", "party2", "seller", "executant_name"],
    },
    "consideration_value": {
        "description": "Agreement or consideration amount.",
        "aliases": ["consideration_value", "agreement_value", "agreement_amount", "transaction_value", "sale_value"],
    },
    "market_value": {
        "description": "Market, guidance, ready reckoner, or valuation amount.",
        "aliases": ["market_value", "ready_reckoner_value", "rr_value", "valuation", "guidance_value"],
    },
    "stamp_duty": {
        "description": "Stamp duty amount paid.",
        "aliases": ["stamp_duty", "stamp_amt", "stamp_amount", "sd_paid", "duty_paid"],
    },
    "registration_fee": {
        "description": "Registration fee amount paid.",
        "aliases": ["registration_fee", "reg_fee", "fee_paid", "registration_charges"],
    },
    "area_sqft": {
        "description": "Property area normalized to square feet.",
        "aliases": ["area_sqft", "carpet_area_sqft", "builtup_area_sqft", "plot_area", "area", "extent"],
    },
    "status": {
        "description": "Document status such as registered, pending, canceled, impounded.",
        "aliases": ["status", "document_status", "remarks", "mutation_flag"],
    },
}

NUMERIC_FIELDS = {"consideration_value", "market_value", "stamp_duty", "registration_fee", "area_sqft"}
DATE_FIELDS = {"registration_date"}
AREA_FACTORS = {
    "hectare": 107639.104,
    "hectares": 107639.104,
    "acre": 43560.0,
    "acres": 43560.0,
    "sq_m": 10.7639,
    "sqm": 10.7639,
    "square_meter": 10.7639,
    "sq_yd": 9.0,
}

logger = logging.getLogger(__name__)

GEMINI_SCHEMA_MAPPING_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "suggestions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_column": {"type": "string"},
                    "canonical_field": {"type": ["string", "null"]},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "reasoning": {"type": "string"},
                },
                "required": ["source_column", "canonical_field", "confidence", "reasoning"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["suggestions"],
    "additionalProperties": False,
}


def canonical_field_descriptions() -> dict[str, str]:
    return {field: spec["description"] for field, spec in CANONICAL_FIELD_SPECS.items()}


def sanitize_dataframe_headers(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    mapping: dict[str, str] = {}
    used: defaultdict[str, int] = defaultdict(int)
    sanitized_headers: list[str] = []

    for idx, original in enumerate(df.columns, start=1):
        original_text = compact_whitespace(str(original))
        base = ascii_slug(original_text, default=f"column_{idx}")
        used[base] += 1
        sanitized = base if used[base] == 1 else f"{base}_{used[base]}"
        sanitized_headers.append(sanitized)
        mapping[original_text] = sanitized

    copy = df.copy()
    copy.columns = sanitized_headers
    return copy, mapping


def infer_dtype(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"

    sample = series.dropna().head(10)
    if sample.empty:
        return "unknown"
    if sample.apply(lambda value: maybe_date_string(value) is not None).sum() >= max(1, len(sample) // 2):
        return "date_like"
    return "string"


def build_column_profiles(
    original_to_sanitized: dict[str, str],
    sanitized_df: pd.DataFrame,
) -> list[ColumnProfile]:
    profiles: list[ColumnProfile] = []
    for original_name, sanitized_name in original_to_sanitized.items():
        series = sanitized_df[sanitized_name]
        sample_values = [
            compact_whitespace(str(value))[:120]
            for value in series.dropna().astype(str).drop_duplicates().head(5).tolist()
        ]
        profiles.append(
            ColumnProfile(
                original_name=original_name,
                sanitized_name=sanitized_name,
                inferred_dtype=infer_dtype(series),
                null_fraction=float(series.isna().mean()) if len(series.index) else 0.0,
                sample_values=sample_values,
            )
        )
    return profiles


def _score_alias_match(sanitized_name: str, aliases: list[str]) -> float:
    score = 0.0
    for alias in aliases:
        alias_slug = ascii_slug(alias)
        if sanitized_name == alias_slug:
            score = max(score, 1.0)
        elif alias_slug in sanitized_name:
            score = max(score, 0.8)
    return score


def heuristic_mapping_decisions(column_profiles: list[ColumnProfile]) -> list[MappingDecision]:
    decisions: list[MappingDecision] = []
    for profile in column_profiles:
        best_field: str | None = None
        best_score = 0.0

        for canonical_field, spec in CANONICAL_FIELD_SPECS.items():
            score = _score_alias_match(profile.sanitized_name, spec["aliases"])

            if canonical_field in DATE_FIELDS and profile.inferred_dtype in {"datetime", "date_like"}:
                score += 0.2
            if canonical_field in NUMERIC_FIELDS and profile.inferred_dtype in {"integer", "float"}:
                score += 0.15
            if canonical_field in {"buyer_name", "seller_name", "office_name", "district"} and profile.inferred_dtype == "string":
                score += 0.05

            if score > best_score:
                best_field = canonical_field
                best_score = score

        if best_score >= 0.75 and best_field:
            decisions.append(
                MappingDecision(
                    canonical_field=best_field,
                    source_column=profile.sanitized_name,
                    confidence=min(best_score, 1.0),
                    strategy="heuristic",
                    notes=f"Matched against aliases for {best_field}.",
                )
            )
        else:
            decisions.append(
                MappingDecision(
                    canonical_field=None,
                    source_column=profile.sanitized_name,
                    confidence=best_score,
                    strategy="unmapped",
                    notes="No heuristic alias match passed the confidence threshold.",
                )
            )
    return decisions


class GeminiSchemaMapper:
    def __init__(self, api_key: str | None, model_name: str) -> None:
        self.api_key = api_key
        self.model_name = model_name
        self.client = genai.Client(api_key=api_key) if api_key and genai else None

    @property
    def is_configured(self) -> bool:
        return self.client is not None

    def suggest(
        self,
        sheet_name: str,
        unresolved_profiles: list[ColumnProfile],
    ) -> list[MappingDecision]:
        if not self.is_configured or not unresolved_profiles:
            return []

        payload = [
            {
                "column": profile.sanitized_name,
                "dtype": profile.inferred_dtype,
                "samples": profile.sample_values,
            }
            for profile in unresolved_profiles
        ]
        prompt = (
            "Map the unresolved source columns to the most likely canonical field if there is a strong semantic fit. "
            "Return null for canonical_field when the column should stay unmapped.\n\n"
            f"Sheet name: {sheet_name}\n"
            f"Canonical fields: {canonical_field_descriptions()}\n"
            f"Columns: {payload}"
        )

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=(
                        "You are a schema-mapping assistant for Maharashtra property registration workbooks. "
                        "Only map a column when the semantic match is strong. Prefer null over guessing."
                    ),
                    temperature=0,
                    response_mime_type="application/json",
                    response_json_schema=GEMINI_SCHEMA_MAPPING_RESPONSE_SCHEMA,
                ),
            )
            parsed = SchemaMappingResponse.model_validate(json.loads(response.text))
            suggestions = parsed.suggestions
        except Exception:
            logger.exception("Gemini schema mapping failed for sheet '%s'. Falling back to heuristic mapping only.", sheet_name)
            return []

        return [
            MappingDecision(
                canonical_field=suggestion.canonical_field,
                source_column=suggestion.source_column,
                confidence=suggestion.confidence,
                strategy="gemini",
                notes=suggestion.reasoning,
            )
            for suggestion in suggestions
        ]


def merge_mapping_decisions(
    heuristic: list[MappingDecision],
    assisted: list[MappingDecision],
) -> list[MappingDecision]:
    by_column = {decision.source_column: decision for decision in heuristic}
    for decision in assisted:
        if decision.source_column not in by_column:
            by_column[decision.source_column] = decision
            continue
        if by_column[decision.source_column].canonical_field is None and decision.canonical_field is not None:
            by_column[decision.source_column] = decision
    return list(by_column.values())


def profile_sheet(
    sheet_name: str,
    raw_table_name: str,
    sanitized_df: pd.DataFrame,
    original_to_sanitized: dict[str, str],
    schema_mapper: GeminiSchemaMapper | None = None,
) -> SheetProfile:
    column_profiles = build_column_profiles(original_to_sanitized, sanitized_df)
    heuristic = heuristic_mapping_decisions(column_profiles)
    unresolved = [
        profile
        for profile in column_profiles
        if next(
            (decision for decision in heuristic if decision.source_column == profile.sanitized_name),
            None,
        )
        and next(
            (decision for decision in heuristic if decision.source_column == profile.sanitized_name),
            None,
        ).canonical_field
        is None
    ]

    assisted = schema_mapper.suggest(sheet_name, unresolved) if schema_mapper else []
    merged = merge_mapping_decisions(heuristic, assisted)
    mapped_fields = sorted({decision.canonical_field for decision in merged if decision.canonical_field})

    sample_rows = [
        {column: jsonable_value(value) for column, value in row.items()}
        for row in sanitized_df.head(3).to_dict(orient="records")
    ]

    return SheetProfile(
        sheet_name=sheet_name,
        raw_table_name=raw_table_name,
        row_count=len(sanitized_df.index),
        column_count=len(sanitized_df.columns),
        column_profiles=column_profiles,
        mapping_decisions=merged,
        mapped_canonical_fields=mapped_fields,
        sample_rows=sample_rows,
    )


def best_mapping_by_canonical(profile: SheetProfile) -> dict[str, MappingDecision]:
    best: dict[str, MappingDecision] = {}
    for decision in profile.mapping_decisions:
        if not decision.canonical_field:
            continue
        existing = best.get(decision.canonical_field)
        if existing is None or decision.confidence > existing.confidence:
            best[decision.canonical_field] = decision
    return best
