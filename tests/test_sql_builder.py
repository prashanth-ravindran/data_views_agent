from data_views_agent.models.contracts import PlanFilter, PlanSort, QueryPlan
from data_views_agent.services.sql_builder import SQLPlanBuilder
from data_views_agent.services.storage import CANONICAL_TABLE


def test_sql_builder_creates_canonical_query() -> None:
    plan = QueryPlan(
        goal="High value Pune flat registrations",
        target_scope="selected_sheets",
        target_sheets=["PUN_001_demo"],
        execution_mode="canonical",
        selected_columns=["office_name", "district", "market_value"],
        filters=[
            PlanFilter(field="district", operator="=", value="Pune"),
            PlanFilter(field="market_value", operator=">", value=10000000),
        ],
        sort=[PlanSort(field="market_value", direction="desc")],
        limit=50,
    )
    builder = SQLPlanBuilder(
        {
            CANONICAL_TABLE: [
                "ingestion_run_id",
                "source_sheet",
                "office_name",
                "district",
                "market_value",
            ]
        }
    )

    built = builder.build(plan, "run123", {"PUN_001_demo": "raw_run123_pun_001_demo"})

    assert 'FROM "canonical_registrations"' in built.sql_text
    assert '"ingestion_run_id" = ?' in built.sql_text
    assert '"source_sheet" IN (?)' in built.sql_text
    assert built.sql_params[0] == "run123"
    assert built.sql_params[-1] == 10000000

