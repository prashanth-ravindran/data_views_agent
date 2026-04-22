# Maharashtra Registration Data Views Agent Spec

## Status
- Implementation started on 2026-04-22.
- This repo uses a local `.venv` created with Python 3.12 because `python3.11` is not installed on this machine.
- The LLM backend is Gemini Flash, not OpenAI.
- The runtime reads `LLM_MODEL` from `.env`, with `GEMINI_MODEL` retained as a compatibility fallback.
- The runtime accepts `LLM_MODEL=gemini-3.1-flash` as a local alias and resolves it to the closest live Gemini `generateContent` model currently exposed by the API: `models/gemini-3-flash-preview`.

## Scope
- Build a Streamlit demo that ingests a large Excel workbook with many sheets, profiles the sheets, normalizes usable fields into SQLite, generates an LLM-driven query plan, allows manual approval/editing of that plan, and returns the filtered dataset.
- Build a standalone Python synthetic data generator for Maharashtra property registration data.
- Keep a data-driven office manifest rather than hardcoding office counts or names inside generator code.

## Current Decisions
- Default Gemini model: `gemini-3.1-flash`.
- Query planning format: structured JSON output from Gemini using the official `google-genai` Python SDK.
- SQL generation: deterministic application code, not model-emitted SQL.
- Storage: SQLite with raw sheet tables, canonical normalized table, ingestion metadata tables, and execution log tables.
- Approval flow: user edits/approves the generated plan in the Streamlit UI before query execution.

## Office Manifest Source Of Truth
- The generator uses current Maharashtra IGR office-address PDFs as the manifest source.
- As of 2026-04-22, the implementation is treating the official PDFs as authoritative over older secondary reporting.
- The checked-in manifest currently contains `489` office records extracted from the seven official division PDFs.
- The synthetic generator defaults to transaction offices only, meaning sub-registrar and joint sub-registrar offices used for registrations, while the full office directory remains available in the manifest.
- Under that default transaction-office filter, the current generator target set is `452` offices.
- Expected manifest fields: `office_code`, `sheet_name`, `office_name`, `division`, `district`, `address`, `email`, `source_url`.

## Implementation Shape
- `app.py`: Streamlit UI.
- `scripts/generate_synthetic_data.py`: standalone dataset generator CLI.
- `scripts/build_office_manifest.py`: regenerates the checked-in office manifest JSON from official PDFs.
- `src/data_views_agent/services/office_manifest.py`: PDF parsing and manifest loading.
- `src/data_views_agent/services/synthetic_data.py`: workbook + metadata generation.
- `src/data_views_agent/services/ingestion.py`: Excel profiling, raw-table loading, canonical normalization.
- `src/data_views_agent/services/planner.py`: Gemini plan generation and optional schema-mapping assistance.
- `src/data_views_agent/services/sql_builder.py`: safe SQL compilation from approved plans.
- `src/data_views_agent/services/storage.py`: SQLite schema, writes, reads, query logs.

## Canonical Query Surface
- Canonical table columns:
  - `ingestion_run_id`
  - `source_sheet`
  - `source_table`
  - `source_row_id`
  - `office_code`
  - `office_name`
  - `division`
  - `district`
  - `registration_date`
  - `document_number`
  - `document_type`
  - `property_type`
  - `property_usage`
  - `locality`
  - `village`
  - `survey_number`
  - `project_name`
  - `buyer_name`
  - `seller_name`
  - `consideration_value`
  - `market_value`
  - `stamp_duty`
  - `registration_fee`
  - `area_sqft`
  - `status`
  - `raw_payload_json`

## Validation Targets
- Office manifest can be regenerated from official PDFs.
- Synthetic generator can create a workbook with one sheet per office in the manifest and a metadata sidecar.
- Ingestion can process heterogeneous schemas sheet-by-sheet without loading the full workbook into memory at once.
- Planner returns structured plans from user prompts.
- Approved plans compile into parameterized `SELECT` SQL only.
- Streamlit app shows the plan, editable JSON, SQL, preview rows, row count, and download options.
