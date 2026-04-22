from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any

from google import genai
from google.genai import types

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
                response_schema=QueryPlan,
                temperature=0,
            ),
        )
        return response.parsed or QueryPlan.model_validate_json(response.text)

