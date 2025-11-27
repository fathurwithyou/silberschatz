import os
import sys
import pytest
from unittest.mock import Mock
import shutil

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
from src.storage.storage_manager import StorageManager
from src.concurrency.concurrency_manager import ConcurrencyControlManager
from src.failure.failure_recovery_manager import FailureRecoveryManager


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _make_mock_storage_manager():
    data_dir = "data_test"
    abs_data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'src', data_dir)
    if os.path.exists(abs_data_path):
        try:
            shutil.rmtree(abs_data_path)
        except Exception:
            pass

    storage = StorageManager(data_directory=data_dir)

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

    # Create table on disk/storage layer
    try:
        storage.create_table(schema)
    except Exception:
        # If already exists, drop and recreate
        try:
            storage.drop_table(schema.table_name)
            storage.create_table(schema)
        except Exception:
            pass

    return storage


def _make_mock_ccm():
    # Use real concurrency control manager (timestamp by default)
    return ConcurrencyControlManager("Timestamp")


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
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(log_path="wal_test.jsonl", storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    # Seed storage with initial row so updates target existing data
    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 2, "name": "A", "salary": 100, "department": "X"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    node = FakeNode("employees", "salary = 65000", "id = 2")
    rows = initial_rows

    result = operator.execute(rows, "salary = 65000", tx_id=1)

    assert result.rows_count == 1
    # verify persisted update
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 2 and r.get("salary") == 65000 for r in persisted.data)


def test_update_multiple_columns():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(log_path="wal_test.jsonl", storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "A", "salary": 10, "department": "X"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    node = FakeNode("employees", "salary = 70000, department = 'Engineering'", "id = 1")
    rows = initial_rows

    result = operator.execute(rows, "salary = 70000, department = 'Engineering'", tx_id=2)
    assert result.rows_count == 1

    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("salary") == 70000 and r.get("department") == "Engineering" for r in persisted.data)


def test_update_all_rows_no_where():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(log_path="wal_test.jsonl", storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1}, {"id": 2}, {"id": 3}], rows_count=3, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    node = FakeNode("employees", "department = 'Marketing'")
    rows = initial_rows

    result = operator.execute(rows, "department = 'Marketing'", tx_id=3)

    # should update 3 rows
    assert result.rows_count == 3
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert all(r.get("department") == "Marketing" for r in persisted.data)


def test_update_with_null_value():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(log_path="wal_test.jsonl", storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "department": "Sales"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    node = FakeNode("employees", "department = NULL", "id = 1")
    rows = initial_rows

    result = operator.execute(rows, "department = NULL", tx_id=4)
    assert result.rows_count == 1

    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("department") is None for r in persisted.data)


def test_update_nonexistent_table_raises_error():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage, FailureRecoveryManager())

    node = FakeNode("nope", "x = 1")
    mock_schema = Mock()
    mock_schema.table_name = "nope"
    mock_schema.primary_key = None  # This will trigger the ValueError
    rows = Rows(data=[], rows_count=0, schema=[mock_schema])

    with pytest.raises(ValueError):
        operator.execute(rows, "x = 1", tx_id=0)


def test_update_nonexistent_column_raises_error():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage, FailureRecoveryManager())

    node = FakeNode("employees", "bad_column = 10")
    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    with pytest.raises(ValueError):
        operator.execute(initial_rows, "bad_column = 10", tx_id=5)


def test_update_integration_flow():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(log_path="wal_test.jsonl", storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 2, "name": "A", "salary": 10, "department": "X"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    node = FakeNode("employees", "salary = 80000, department = 'IT'", "id = 2")
    rows = initial_rows

    result = operator.execute(rows, "salary = 80000, department = 'IT'", tx_id=6)

    assert result.rows_count == 1
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 2 and r.get("salary") == 80000 and r.get("department") == "IT" for r in persisted.data)
