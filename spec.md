# Maharashtra Registration Data Views Agent Spec

## Status
- Implementation started on 2026-04-22.
- This repo uses a local `.venv` created with Python 3.12 because `python3.11` is not installed on this machine.
- The planning assistant reads its model setting from `LLM_MODEL` in `.env`.
- The app uses neutral, non-technical language in the demo UI and hides implementation details from the audience view.

## Scope
- Build a Streamlit demo that ingests a large Excel workbook with many sheets, profiles the sheets, creates a shared analysis view across tabs, generates an AI-assisted retrieval proposal, allows manual approval/editing of that proposal, and returns the filtered dataset.
- Build a standalone Python synthetic data generator for Maharashtra property registration data.
- Keep a data-driven office manifest rather than hardcoding office counts or names inside generator code.

## Current Decisions
- Model selection is controlled through `LLM_MODEL` in `.env`.
- Proposal generation returns structured plan data that the app can review and execute safely.
- Result filtering is compiled deterministically in application code rather than by free-form model output.
- Data is prepared in a local analysis workspace with source-tab lineage and run history.
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
- `src/data_views_agent/services/ingestion.py`: Excel profiling, tab loading, and shared analysis view creation.
- `src/data_views_agent/services/planner.py`: proposal generation and optional field-mapping assistance.
- `src/data_views_agent/services/sql_builder.py`: safe filter compilation from approved proposals.
- `src/data_views_agent/services/storage.py`: local workspace persistence, retrieval, and run history.

## Shared Analysis Fields
- Shared analysis fields:
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
- Proposal generation returns structured plans from user prompts.
- Approved plans compile into safe filtered retrievals only.
- Streamlit app shows the proposal summary, optional advanced review, preview rows, row count, and download options.
