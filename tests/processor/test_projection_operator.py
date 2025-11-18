import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.core.models import (
    Rows,
    TableSchema,
    ColumnDefinition,
    DataType,
)
from src.processor.operators import ProjectionOperator


def _make_users_rows() -> Rows:
    schema = TableSchema(
        table_name="users",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
        ],
        primary_key="id",
    )
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 24},
    ]
    return Rows(data=data, rows_count=len(data), schema=[schema])


def _make_joined_rows() -> Rows:
    user_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnDefinition(
                name="user_id", data_type=DataType.INTEGER, primary_key=True
            ),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
        ],
        primary_key="user_id",
    )
    order_schema = TableSchema(
        table_name="orders",
        columns=[
            ColumnDefinition(
                name="order_id", data_type=DataType.INTEGER, primary_key=True
            ),
            ColumnDefinition(name="amount", data_type=DataType.FLOAT),
        ],
        primary_key="order_id",
    )

    data = [
        {"user_id": 1, "name": "Alice", "order_id": 10, "amount": 99.5},
    ]

    return Rows(data=data, rows_count=len(data), schema=[user_schema, order_schema])


def test_projection_wildcard_returns_original_rows():
    operator = ProjectionOperator()
    rows = _make_users_rows()

    result = operator.execute(rows, "*")

    assert result is rows


def test_projection_select_specific_columns():
    operator = ProjectionOperator()
    rows = _make_users_rows()

    result = operator.execute(rows, "id, name")

    assert result.rows_count == rows.rows_count
    assert all(set(row.keys()) == {"id", "name"} for row in result.data)
    assert [col.name for col in result.schema[0].columns] == ["id", "name"]


def test_projection_applies_alias():
    operator = ProjectionOperator()
    rows = _make_users_rows()

    result = operator.execute(rows, "users.id AS user_id")

    assert all("user_id" in row and "id" not in row for row in result.data)
    assert result.schema[0].columns[0].name == "user_id"


def test_projection_table_wildcard_and_column():
    operator = ProjectionOperator()
    rows = _make_joined_rows()

    result = operator.execute(rows, "users.*, orders.order_id")

    assert list(result.data[0].keys()) == ["user_id", "name", "order_id"]
    assert [col.name for col in result.schema[0].columns] == ["user_id", "name"]
    assert [col.name for col in result.schema[1].columns] == ["order_id"]


def test_projection_unknown_column_raises():
    operator = ProjectionOperator()
    rows = _make_users_rows()

    with pytest.raises(ValueError):
        operator.execute(rows, "nonexistent")
