from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from data_views_agent.config import get_settings
from data_views_agent.models.contracts import QueryPlan, WorkbookProfile
from data_views_agent.services.ingestion import WorkbookIngestionService
from data_views_agent.services.office_manifest import ensure_office_manifest
from data_views_agent.services.planner import GeminiPlanner
from data_views_agent.services.profiling import GeminiSchemaMapper
from data_views_agent.services.sql_builder import SQLBuildError, SQLPlanBuilder
from data_views_agent.services.storage import CANONICAL_TABLE, SQLiteStore
from data_views_agent.services.synthetic_data import dataframe_to_xlsx_bytes, generate_workbook


st.set_page_config(page_title="Maharashtra Data Views Agent", layout="wide")

settings = get_settings()
store = SQLiteStore(settings.database_path)
schema_mapper = GeminiSchemaMapper(settings.gemini_api_key, settings.gemini_model)
planner = GeminiPlanner(settings.gemini_api_key, settings.gemini_model)
ingestor = WorkbookIngestionService(store, schema_mapper=schema_mapper)


def current_profile() -> WorkbookProfile | None:
    return st.session_state.get("current_profile")


def save_and_ingest(workbook_path: Path) -> None:
    with st.spinner("Profiling workbook and loading it into SQLite..."):
        profile = ingestor.ingest_workbook(workbook_path)
    st.session_state["current_profile"] = profile
    st.session_state["generated_plan_json"] = ""
    st.session_state["approved_plan_json"] = ""
    st.session_state["last_prompt"] = ""


def generate_demo_workbook(rows: int, offices: int, seed: int) -> Path:
    manifest = ensure_office_manifest(settings.office_manifest_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    workbook_path = settings.generated_dir / f"demo_{timestamp}.xlsx"
    metadata_path = settings.generated_dir / f"demo_{timestamp}.metadata.json"
    with st.spinner("Generating synthetic workbook..."):
        generate_workbook(
            manifest,
            workbook_path,
            metadata_path,
            total_rows=rows,
            seed=seed,
            max_offices=offices,
        )
    return workbook_path


def render_profile(profile: WorkbookProfile) -> None:
    st.subheader("Ingestion Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Workbook", profile.workbook_name)
    col2.metric("Sheets", profile.total_sheets)
    col3.metric("Rows", profile.total_rows)

    profile_rows = [
        {
            "sheet_name": sheet.sheet_name,
            "rows": sheet.row_count,
            "columns": sheet.column_count,
            "mapped_canonical_fields": ", ".join(sheet.mapped_canonical_fields),
        }
        for sheet in profile.sheet_profiles[:50]
    ]
    st.dataframe(profile_rows, use_container_width=True, hide_index=True)


def render_plan_summary(plan: QueryPlan) -> None:
    st.markdown("**Generated Plan**")
    st.write(f"Goal: {plan.goal}")
    st.write(f"Execution mode: `{plan.execution_mode}`")
    st.write(f"Target scope: `{plan.target_scope}`")
    st.write(f"Target sheets: {', '.join(plan.target_sheets) if plan.target_sheets else 'All applicable sheets'}")
    st.write(f"Selected columns: {', '.join(plan.selected_columns) if plan.selected_columns else 'Default columns'}")
    if plan.filters:
        st.write(
            "Filters: "
            + "; ".join(f"{flt.field} {flt.operator} {flt.value}" for flt in plan.filters)
        )
    if plan.assumptions:
        st.write("Assumptions: " + "; ".join(plan.assumptions))
    if plan.ambiguities:
        st.write("Ambiguities: " + "; ".join(plan.ambiguities))
    if plan.explanation:
        st.write(plan.explanation)


def available_columns_for_profile(profile: WorkbookProfile) -> tuple[dict[str, list[str]], dict[str, str]]:
    sheet_table_map = {sheet.sheet_name: sheet.raw_table_name for sheet in profile.sheet_profiles}
    available = {CANONICAL_TABLE: store.get_table_columns(CANONICAL_TABLE)}
    for raw_table_name in sheet_table_map.values():
        available[raw_table_name] = store.get_table_columns(raw_table_name)
    return available, sheet_table_map


st.title("Maharashtra Registration Data Views Agent")
st.caption("Gemini-planned, SQLite-backed filtering for large multi-sheet Excel workbooks.")

with st.expander("Environment", expanded=True):
    col1, col2, col3 = st.columns(3)
    col1.write(f"Gemini model: `{settings.gemini_model}`")
    col2.write(f"Gemini configured: `{planner.is_configured}`")
    col3.write(f"SQLite DB: `{settings.database_path}`")
    if not planner.is_configured:
        st.warning("Set `GEMINI_API_KEY` or `GOOGLE_API_KEY` in `.env` before generating plans.")

st.subheader("Data Input")
upload_col, demo_col = st.columns(2)

with upload_col:
    uploaded_file = st.file_uploader("Upload an Excel workbook", type=["xlsx", "xlsm"])
    if uploaded_file and st.button("Ingest Uploaded Workbook", use_container_width=True):
        upload_path = settings.uploads_dir / f"{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uploaded_file.name}"
        upload_path.write_bytes(uploaded_file.getvalue())
        save_and_ingest(upload_path)

with demo_col:
    demo_rows = st.number_input("Demo rows", min_value=1000, max_value=100000, value=15000, step=1000)
    demo_offices = st.number_input("Demo office sheets", min_value=5, max_value=100, value=25, step=5)
    demo_seed = st.number_input("Random seed", min_value=1, max_value=9999, value=7, step=1)
    if st.button("Generate And Ingest Demo Workbook", use_container_width=True):
        workbook_path = generate_demo_workbook(demo_rows, demo_offices, demo_seed)
        save_and_ingest(workbook_path)

profile = current_profile()
if profile:
    render_profile(profile)

    st.subheader("Prompt To Plan")
    prompt = st.text_area(
        "Describe the data subset you want",
        value=st.session_state.get("last_prompt", ""),
        height=140,
        placeholder="Example: Show registrations from Pune district after 2024-01-01 where market value is above 1 crore and property type is Flat.",
    )
    if st.button("Generate Plan", disabled=not planner.is_configured or not prompt.strip()):
        plan = planner.generate_plan(prompt.strip(), profile.ingestion_run_id, store)
        st.session_state["last_prompt"] = prompt.strip()
        st.session_state["generated_plan_json"] = plan.model_dump_json(indent=2)
        st.session_state["approved_plan_json"] = st.session_state["generated_plan_json"]

    if st.session_state.get("generated_plan_json"):
        generated_plan = QueryPlan.model_validate_json(st.session_state["generated_plan_json"])
        render_plan_summary(generated_plan)
        st.text_area("Editable approved plan JSON", key="approved_plan_json", height=320)

        if st.button("Approve And Run Query", use_container_width=True):
            try:
                approved_plan = QueryPlan.model_validate_json(st.session_state["approved_plan_json"])
                available_columns, sheet_table_map = available_columns_for_profile(profile)
                built_query = SQLPlanBuilder(available_columns).build(
                    approved_plan,
                    profile.ingestion_run_id,
                    sheet_table_map,
                )
                row_count = store.count_query(built_query.count_sql, built_query.count_params)
                result_df = store.execute_query(built_query.sql_text, built_query.sql_params)
                store.log_query(
                    profile.ingestion_run_id,
                    st.session_state["last_prompt"],
                    st.session_state["generated_plan_json"],
                    st.session_state["approved_plan_json"],
                    built_query.sql_text,
                    built_query.sql_params,
                    row_count,
                )
                st.success(f"Query returned {row_count} rows.")
                st.code(built_query.sql_text, language="sql")
                st.write({"sql_params": built_query.sql_params})
                st.dataframe(result_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download CSV",
                    data=result_df.to_csv(index=False).encode("utf-8"),
                    file_name="filtered_data.csv",
                    mime="text/csv",
                )
                st.download_button(
                    "Download XLSX",
                    data=dataframe_to_xlsx_bytes(result_df),
                    file_name="filtered_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except (SQLBuildError, ValueError) as exc:
                st.error(str(exc))

        history = store.fetch_recent_query_logs(profile.ingestion_run_id, limit=10)
        if not history.empty:
            st.subheader("Execution History")
            st.dataframe(history, use_container_width=True, hide_index=True)
