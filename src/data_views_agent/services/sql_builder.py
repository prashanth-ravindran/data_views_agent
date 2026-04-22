from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data_views_agent.models.contracts import PlanFilter, QueryPlan
from data_views_agent.services.storage import CANONICAL_TABLE


@dataclass
class BuiltQuery:
    sql_text: str
    sql_params: list[Any]
    count_sql: str
    count_params: list[Any]
    selected_columns: list[str]
    table_name: str


class SQLBuildError(ValueError):
    pass


def quote_identifier(identifier: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise SQLBuildError(f"Unsafe identifier: {identifier}")
    return f'"{identifier}"'


class SQLPlanBuilder:
    def __init__(self, available_columns: dict[str, list[str]]) -> None:
        self.available_columns = available_columns

    def _validate_columns(self, table_name: str, columns: list[str]) -> list[str]:
        valid_columns = set(self.available_columns[table_name])
        invalid = [column for column in columns if column not in valid_columns]
        if invalid:
            raise SQLBuildError(f"Unknown columns for {table_name}: {invalid}")
        return columns

    def _condition_for_filter(self, condition: PlanFilter, valid_columns: set[str]) -> tuple[str, list[Any]]:
        if condition.field not in valid_columns:
            raise SQLBuildError(f"Unknown filter column: {condition.field}")

        field_sql = quote_identifier(condition.field)
        operator = condition.operator

        if operator == "is_null":
            return f"{field_sql} IS NULL", []
        if operator == "not_null":
            return f"{field_sql} IS NOT NULL", []
        if operator == "contains":
            return f"LOWER(COALESCE({field_sql}, '')) LIKE LOWER(?)", [f"%{condition.value}%"]
        if operator == "in":
            if not isinstance(condition.value, list) or not condition.value:
                raise SQLBuildError("The 'in' operator requires a non-empty list value.")
            placeholders = ", ".join("?" for _ in condition.value)
            return f"{field_sql} IN ({placeholders})", [self._coerce_literal(value) for value in condition.value]
        if operator == "between":
            if not isinstance(condition.value, list) or len(condition.value) != 2:
                raise SQLBuildError("The 'between' operator requires exactly two values.")
            return f"{field_sql} BETWEEN ? AND ?", [self._coerce_literal(value) for value in condition.value]
        if operator in {"=", "!=", ">", ">=", "<", "<="}:
            return f"{field_sql} {operator} ?", [self._coerce_literal(condition.value)]
        raise SQLBuildError(f"Unsupported operator: {operator}")

    @staticmethod
    def _coerce_literal(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        stripped = value.strip()
        if not stripped:
            return stripped
        try:
            if "." in stripped:
                return float(stripped)
            return int(stripped)
        except ValueError:
            return stripped

    def build(
        self,
        plan: QueryPlan,
        ingestion_run_id: str,
        sheet_name_to_table: dict[str, str],
    ) -> BuiltQuery:
        if plan.execution_mode == "raw_sheet":
            if len(plan.target_sheets) != 1:
                raise SQLBuildError("Raw-sheet mode requires exactly one target sheet.")
            sheet_name = plan.target_sheets[0]
            if sheet_name not in sheet_name_to_table:
                raise SQLBuildError(f"Unknown target sheet: {sheet_name}")
            table_name = sheet_name_to_table[sheet_name]
            base_conditions: list[str] = []
            sql_params: list[Any] = []
        else:
            table_name = CANONICAL_TABLE
            base_conditions = ['"ingestion_run_id" = ?']
            sql_params = [ingestion_run_id]
            if plan.target_scope == "selected_sheets" and plan.target_sheets:
                placeholders = ", ".join("?" for _ in plan.target_sheets)
                base_conditions.append(f'"source_sheet" IN ({placeholders})')
                sql_params.extend(plan.target_sheets)

        available = self.available_columns[table_name]
        valid_columns = set(available)
        selected_columns = plan.selected_columns or available[: min(len(available), 12)]
        self._validate_columns(table_name, selected_columns)

        clauses = list(base_conditions)
        filter_params: list[Any] = []
        for condition in plan.filters:
            clause, params = self._condition_for_filter(condition, valid_columns)
            clauses.append(clause)
            filter_params.extend(params)

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order_sql = ""
        if plan.sort:
            order_parts = []
            for sort in plan.sort:
                if sort.field not in valid_columns:
                    raise SQLBuildError(f"Unknown sort column: {sort.field}")
                direction = "DESC" if sort.direction.lower() == "desc" else "ASC"
                order_parts.append(f"{quote_identifier(sort.field)} {direction}")
            order_sql = " ORDER BY " + ", ".join(order_parts)

        limit = max(1, min(plan.limit or 200, 10000))
        quoted_columns = ", ".join(quote_identifier(column) for column in selected_columns)
        sql_text = f"SELECT {quoted_columns} FROM {quote_identifier(table_name)}{where_sql}{order_sql} LIMIT {limit}"
        count_sql = f"SELECT COUNT(*) FROM {quote_identifier(table_name)}{where_sql}"
        final_params = [*sql_params, *filter_params]

        return BuiltQuery(
            sql_text=sql_text,
            sql_params=final_params,
            count_sql=count_sql,
            count_params=final_params,
            selected_columns=selected_columns,
            table_name=table_name,
        )
