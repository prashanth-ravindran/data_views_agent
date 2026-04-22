from __future__ import annotations

import json
from difflib import SequenceMatcher
from typing import Any

from google import genai
from google.genai import types
from pydantic import BaseModel, Field

from data_views_agent.models.contracts import QueryPlan
from data_views_agent.services.profiling import canonical_field_descriptions
from data_views_agent.services.storage import SQLiteStore


PLANNER_SYSTEM_INSTRUCTION = """
You are a planning agent for a Maharashtra property-registration data explorer.
Produce a structured retrieval plan, not prose.
Prefer execution_mode='canonical' unless the prompt explicitly requires a raw sheet-specific column.
Never invent fields or sheet names.
If the prompt is ambiguous, preserve that ambiguity in the plan and set needs_user_confirmation=true.
Keep the plan executable by a deterministic SQL builder.
""".strip()


class GeminiPlanFilter(BaseModel):
    field: str
    operator: str
    value_text: str | None = None
    values: list[str] = Field(default_factory=list)
    rationale: str = ""


class GeminiPlanSort(BaseModel):
    field: str
    direction: str = "asc"


class GeminiQueryPlan(BaseModel):
    goal: str
    target_scope: str = "all_sheets"
    target_sheets: list[str] = Field(default_factory=list)
    execution_mode: str = "canonical"
    selected_columns: list[str] = Field(default_factory=list)
    filters: list[GeminiPlanFilter] = Field(default_factory=list)
    sort: list[GeminiPlanSort] = Field(default_factory=list)
    limit: int = 200
    assumptions: list[str] = Field(default_factory=list)
    ambiguities: list[str] = Field(default_factory=list)
    needs_user_confirmation: bool = True
    explanation: str = ""


GEMINI_QUERY_PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "goal": {"type": "string", "description": "Short restatement of the retrieval goal."},
        "target_scope": {
            "type": "string",
            "enum": ["all_sheets", "selected_sheets"],
            "description": "Whether the query should run across all sheets or only named sheets.",
        },
        "target_sheets": {"type": "array", "items": {"type": "string"}},
        "execution_mode": {
            "type": "string",
            "enum": ["canonical", "raw_sheet"],
            "description": "Prefer canonical unless the prompt clearly requires raw sheet columns.",
        },
        "selected_columns": {"type": "array", "items": {"type": "string"}},
        "filters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "operator": {
                        "type": "string",
                        "enum": ["=", "!=", ">", ">=", "<", "<=", "contains", "in", "between", "is_null", "not_null"],
                    },
                    "value_text": {"type": ["string", "null"]},
                    "values": {"type": "array", "items": {"type": "string"}},
                    "rationale": {"type": "string"},
                },
                "required": ["field", "operator", "value_text", "values", "rationale"],
                "additionalProperties": False,
            },
        },
        "sort": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "direction": {"type": "string", "enum": ["asc", "desc"]},
                },
                "required": ["field", "direction"],
                "additionalProperties": False,
            },
        },
        "limit": {"type": "integer", "minimum": 1, "maximum": 10000},
        "assumptions": {"type": "array", "items": {"type": "string"}},
        "ambiguities": {"type": "array", "items": {"type": "string"}},
        "needs_user_confirmation": {"type": "boolean"},
        "explanation": {"type": "string"},
    },
    "required": [
        "goal",
        "target_scope",
        "target_sheets",
        "execution_mode",
        "selected_columns",
        "filters",
        "sort",
        "limit",
        "assumptions",
        "ambiguities",
        "needs_user_confirmation",
        "explanation",
    ],
    "additionalProperties": False,
}


class GeminiPlanner:
    def __init__(self, api_key: str | None, model_name: str) -> None:
        self.api_key = api_key
        self.model_name = model_name
        self.client = genai.Client(api_key=api_key) if api_key else None

    @property
    def is_configured(self) -> bool:
        return self.client is not None

    def _matching_sheets(self, prompt: str, sheet_catalog: list[dict[str, Any]]) -> list[str]:
        prompt_lower = prompt.lower()
        ranked = []
        for entry in sheet_catalog:
            sheet_name = entry["sheet_name"]
            similarity = SequenceMatcher(None, prompt_lower, sheet_name.lower()).ratio()
            if sheet_name.lower() in prompt_lower:
                similarity += 1.0
            ranked.append((similarity, entry["row_count"], sheet_name))
        ranked.sort(reverse=True)
        return [sheet_name for score, _, sheet_name in ranked if score > 0.15][:8]

    def build_context(self, prompt: str, ingestion_run_id: str, store: SQLiteStore) -> dict[str, Any]:
        run_summary = store.current_run_summary(ingestion_run_id)
        sheet_catalog = store.fetch_sheet_catalog(ingestion_run_id)
        matching_sheets = self._matching_sheets(prompt, sheet_catalog)
        selected_sheets = matching_sheets or [entry["sheet_name"] for entry in sorted(sheet_catalog, key=lambda item: item["row_count"], reverse=True)[:8]]

        return {
            "run_summary": run_summary,
            "canonical_fields": canonical_field_descriptions(),
            "distinct_divisions": store.fetch_distinct_values(ingestion_run_id, "division"),
            "distinct_districts": store.fetch_distinct_values(ingestion_run_id, "district"),
            "distinct_document_types": store.fetch_distinct_values(ingestion_run_id, "document_type"),
            "distinct_property_types": store.fetch_distinct_values(ingestion_run_id, "property_type"),
            "distinct_offices": store.fetch_distinct_values(ingestion_run_id, "office_name", limit=40),
            "sheet_summaries": [entry for entry in sheet_catalog if entry["sheet_name"] in selected_sheets],
            "sheet_columns": store.fetch_sheet_columns(ingestion_run_id, selected_sheets[:5]),
        }

    @staticmethod
    def _to_internal_plan(plan: GeminiQueryPlan) -> QueryPlan:
        filters = []
        for flt in plan.filters:
            if flt.operator in {"in", "between"}:
                value: str | list[str] | None = flt.values
            elif flt.operator in {"is_null", "not_null"}:
                value = None
            else:
                value = flt.value_text

            filters.append(
                {
                    "field": flt.field,
                    "operator": flt.operator,
                    "value": value,
                    "rationale": flt.rationale,
                }
            )

        return QueryPlan.model_validate(
            {
                "goal": plan.goal,
                "target_scope": plan.target_scope,
                "target_sheets": plan.target_sheets,
                "execution_mode": plan.execution_mode,
                "selected_columns": plan.selected_columns,
                "filters": filters,
                "sort": [item.model_dump() for item in plan.sort],
                "limit": plan.limit,
                "assumptions": plan.assumptions,
                "ambiguities": plan.ambiguities,
                "needs_user_confirmation": plan.needs_user_confirmation,
                "explanation": plan.explanation,
            }
        )

    def generate_plan(self, prompt: str, ingestion_run_id: str, store: SQLiteStore) -> QueryPlan:
        if not self.client:
            raise RuntimeError("Gemini API key is not configured.")

        context = self.build_context(prompt, ingestion_run_id, store)
        request = (
            "User prompt:\n"
            f"{prompt}\n\n"
            "Dataset context:\n"
            f"{context}\n\n"
            "Return a query plan that can be turned into a SQLite SELECT query."
        )

        response = self.client.models.generate_content(
            model=self.model_name,
            contents=request,
            config=types.GenerateContentConfig(
                system_instruction=PLANNER_SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
                response_json_schema=GEMINI_QUERY_PLAN_SCHEMA,
                temperature=0,
            ),
        )
        parsed = GeminiQueryPlan.model_validate(json.loads(response.text))
        return self._to_internal_plan(parsed)
