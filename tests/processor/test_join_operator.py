import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.core.models import (
    Rows,
    TableSchema,
    ColumnDefinition,
    DataType,
)
from src.processor.operators import JoinOperator


def _make_rows(table_name: str, columns: list[ColumnDefinition], data: list[dict]) -> Rows:
    schema = TableSchema(table_name=table_name, columns=columns)
    return Rows(data=data, rows_count=len(data), schema=[schema])


def test_join_with_condition_filters_rows():
    join_operator = JoinOperator()
    users_rows = _make_rows(
        "users",
        [
            ColumnDefinition(name="id", data_type=DataType.INTEGER),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
        ],
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ],
    )
    orders_rows = _make_rows(
        "orders",
        [
            ColumnDefinition(name="order_id", data_type=DataType.INTEGER),
            ColumnDefinition(name="user_id", data_type=DataType.INTEGER),
        ],
        [
            {"order_id": 10, "user_id": 1},
            {"order_id": 11, "user_id": 3},
        ],
    )

    result = join_operator.execute(users_rows, orders_rows, "users.id = orders.user_id")

    assert result.rows_count == 1
    row = result.data[0]
    assert row["users.id"] == 1
    assert row["orders.user_id"] == 1
    assert row["orders.order_id"] == 10


def test_join_without_condition_produces_cartesian_product():
    join_operator = JoinOperator()
    left_rows = _make_rows(
        "a",
        [ColumnDefinition(name="id", data_type=DataType.INTEGER)],
        [{"id": 1}, {"id": 2}],
    )
    right_rows = _make_rows(
        "b",
        [ColumnDefinition(name="value", data_type=DataType.INTEGER)],
        [{"value": 10}],
    )

    result = join_operator.execute(left_rows, right_rows, None)

    assert result.rows_count == 2
    assert all("b.value" in row for row in result.data)


def test_join_preserves_duplicate_column_names_with_qualifiers():
    join_operator = JoinOperator()
    left_rows = _make_rows(
        "users",
        [ColumnDefinition(name="id", data_type=DataType.INTEGER)],
        [{"id": 1}],
    )
    right_rows = _make_rows(
        "orders",
        [ColumnDefinition(name="id", data_type=DataType.INTEGER)],
        [{"id": 2}],
    )

    result = join_operator.execute(left_rows, right_rows, None)
    assert result.rows_count == 1
    row = result.data[0]
    assert row["users.id"] == 1
    assert row["orders.id"] == 2
