import os
import sys
import pytest
from unittest.mock import Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.core.models import (
    Rows,
    TableSchema,
    ColumnDefinition,
    DataType,
    DataWrite,
    Condition,
)
from src.processor.operators import UpdateOperator


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _make_mock_storage_manager():
    storage = Mock()

    schema = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="salary", data_type=DataType.INTEGER),
            ColumnDefinition(name="department", data_type=DataType.VARCHAR, max_length=30),
        ],
        primary_key="id",
    )

    storage.get_table_schema = Mock(return_value=schema)
    storage.write_block = Mock(return_value=1)

    return storage


def _make_mock_ccm():
    ccm = Mock()
    ccm.validate_object = Mock()
    return ccm


class FakeNode:
    """Node sederhana meniru QueryTree untuk UPDATE."""

    def __init__(self, table_name, set_clause, where_clause=None):
        from src.core.models.query import QueryNodeType, QueryTree

        # Node TABLE paling bawah
        self.table_node = QueryTree(
            type=QueryNodeType.TABLE,
            value=table_name,
            children=[]
        )

        # Node SELECTION (jika WHERE ada)
        if where_clause:
            self.selection_node = QueryTree(
                type=QueryNodeType.SELECTION,
                value=where_clause,
                children=[self.table_node]
            )
            child = self.selection_node
        else:
            child = self.table_node

        # Node UPDATE paling atas
        self.update_node = QueryTree(
            type=QueryNodeType.UPDATE,
            value=set_clause,      # SET clause
            children=[child]
        )

        self.value = set_clause
        self.children = [child]


# -------------------------------------------------------------------------
# TEST CASES
# -------------------------------------------------------------------------

def test_update_single_column():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("employees", "salary = 65000", "id = 2")
    rows = [{"id": 2, "name": "A", "salary": 100, "department": "X"}]

    result = operator.execute(node, rows)

    assert result["updated_rows"] == 1
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["salary"] == 65000


def test_update_multiple_columns():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("employees", "salary = 70000, department = 'Engineering'", "id = 1")
    rows = [{"id": 1, "name": "A", "salary": 10, "department": "X"}]

    result = operator.execute(node, rows)
    assert result["updated_rows"] == 1

    call = storage.write_block.call_args[0][0]
    assert call.data["salary"] == 70000
    assert call.data["department"] == "Engineering"


def test_update_all_rows_no_where():
    storage = _make_mock_storage_manager()
    storage.write_block = Mock(return_value=3)  # tiap write memberi 3
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("employees", "department = 'Marketing'")
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]

    result = operator.execute(node, rows)

    # 3 rows × write_block return(3) = 9
    assert result["updated_rows"] == 9


def test_update_with_null_value():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("employees", "department = NULL", "id = 1")
    rows = [{"id": 1, "department": "Sales"}]

    result = operator.execute(node, rows)
    assert result["updated_rows"] == 1

    call = storage.write_block.call_args[0][0]
    assert call.data["department"] is None


def test_update_nonexistent_table_raises_error():
    storage = _make_mock_storage_manager()
    storage.get_table_schema = Mock(return_value=None)
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("nope", "x = 1")

    with pytest.raises(AttributeError):
        operator.execute(node, [])


def test_update_nonexistent_column_raises_error():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("employees", "bad_column = 10")

    rows = [{"id": 1}]
    with pytest.raises(ValueError):
        operator.execute(node, rows)


def test_update_integration_flow():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage)

    node = FakeNode("employees", "salary = 80000, department = 'IT'", "id = 2")
    rows = [{"id": 2, "name": "A", "salary": 10, "department": "X"}]

    result = operator.execute(node, rows)

    assert result["updated_rows"] == 1
    call = storage.write_block.call_args[0][0]
    assert call.data["salary"] == 80000
    assert call.data["department"] == "IT"
