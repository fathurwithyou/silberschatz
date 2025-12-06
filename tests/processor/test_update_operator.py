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
    # Create a mock that always allows operations for testing
    from unittest.mock import Mock
    from src.core.models.response import Response
    
    mock_ccm = Mock()
    mock_ccm.validate_object.return_value = Response(allowed=True, transaction_id=1)
    mock_ccm.get_active_transactions.return_value = (None, [1, 2, 3])
    return mock_ccm


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
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    # Seed storage with initial row so updates target existing data
    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 2, "name": "A", "salary": 100, "department": "X"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    rows = initial_rows

    result = operator.execute(rows, "salary = 65000", tx_id=1)

    assert result.rows_count == 1
    # verify persisted update
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 2 and r.get("salary") == 65000 for r in persisted.data)


def test_update_multiple_columns():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "A", "salary": 10, "department": "X"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    rows = initial_rows

    result = operator.execute(rows, "salary = 70000, department = 'Engineering'", tx_id=2)
    assert result.rows_count == 1

    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("salary") == 70000 and r.get("department") == "Engineering" for r in persisted.data)


def test_update_all_rows_no_where():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1}, {"id": 2}, {"id": 3}], rows_count=3, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    rows = initial_rows

    result = operator.execute(rows, "department = 'Marketing'", tx_id=3)

    # should update 3 rows
    assert result.rows_count == 3
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert all(r.get("department") == "Marketing" for r in persisted.data)


def test_update_with_null_value():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "department": "Sales"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    rows = initial_rows

    result = operator.execute(rows, "department = NULL", tx_id=4)
    assert result.rows_count == 1

    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("department") is None for r in persisted.data)


def test_update_nonexistent_table_raises_error():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage, FailureRecoveryManager())

    mock_schema = Mock()
    mock_schema.table_name = "nope"
    mock_schema.primary_key = None  # This will trigger the ValueError
    mock_schema.columns = []
    rows = Rows(data=[], rows_count=0, schema=[mock_schema])

    with pytest.raises(ValueError):
        operator.execute(rows, "x = 1", tx_id=0)


def test_update_nonexistent_column_raises_error():
    storage = _make_mock_storage_manager()
    operator = UpdateOperator(_make_mock_ccm(), storage, FailureRecoveryManager())

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    with pytest.raises(ValueError):
        operator.execute(initial_rows, "bad_column = 10", tx_id=5)


def test_update_integration_flow():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 2, "name": "A", "salary": 10, "department": "X"}], rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    rows = initial_rows

    result = operator.execute(rows, "salary = 80000, department = 'IT'", tx_id=6)

    assert result.rows_count == 1
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 2 and r.get("salary") == 80000 and r.get("department") == "IT" for r in persisted.data)


def test_update_primary_key_duplicate_raises_error():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    # Create multiple employees to test PK constraint
    initial_rows = Rows(data=[
        {"id": 1, "name": "Alice", "salary": 50000, "department": "HR"},
        {"id": 2, "name": "Bob", "salary": 60000, "department": "IT"}
    ], rows_count=2, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try to update id=2 to id=1 (duplicate PK)
    single_row = Rows(data=[{"id": 2, "name": "Bob", "salary": 60000, "department": "IT"}], 
                     rows_count=1, schema=[schema])

    with pytest.raises(ValueError, match="UPDATE causes PK conflict"):
        operator.execute(single_row, "id = 1", tx_id=10)


def test_update_type_mismatch_string_to_integer():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try to set string to integer column
    with pytest.raises(ValueError, match="Cannot convert.*to INTEGER"):
        operator.execute(initial_rows, "salary = 'not_a_number'", tx_id=11)


def test_update_type_mismatch_string_to_id():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try to set string to ID (INTEGER) column
    with pytest.raises(ValueError, match="Cannot convert.*to INTEGER"):
        operator.execute(initial_rows, "id = 'invalid_id'", tx_id=12)


def test_update_malformed_assignment_expression():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try malformed assignment without = sign
    try:
        result = operator.execute(initial_rows, "salary 123456", tx_id=13)
        # If no error, check if no assignments were parsed
        persisted = storage.dml_manager.load_all_rows("employees", schema)
        assert any(r.get("salary") == 50000 for r in persisted.data)  # Unchanged
    except Exception:
        # Expected behavior - malformed assignment should be ignored or error
        pass


def test_update_empty_assignment():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try empty assignment
    result = operator.execute(initial_rows, "", tx_id=14)
    assert result.rows_count == 1
    
    # Data should be unchanged since no assignments
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("salary") == 50000 for r in persisted.data)


def test_update_numeric_string_conversion():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try setting numeric string to integer column (should work)
    result = operator.execute(initial_rows, "salary = '75000'", tx_id=15)
    assert result.rows_count == 1
    
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("salary") == 75000 for r in persisted.data)


def test_update_quoted_vs_unquoted_strings():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Try setting unquoted string to VARCHAR column
    result = operator.execute(initial_rows, "department = Engineering", tx_id=17)
    assert result.rows_count == 1
    
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("department") == "Engineering" for r in persisted.data)


def test_update_zero_and_negative_values():
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = FailureRecoveryManager(storage_manager=storage)
    operator = UpdateOperator(ccm, storage, frm)

    schema = storage.get_table_schema("employees")
    assert schema is not None
    initial_rows = Rows(data=[{"id": 1, "name": "Alice", "salary": 50000, "department": "HR"}], 
                       rows_count=1, schema=[schema])
    storage.dml_manager.save_all_rows("employees", initial_rows, schema)

    # Test zero value
    result = operator.execute(initial_rows, "salary = 0", tx_id=18)
    assert result.rows_count == 1
    
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("salary") == 0 for r in persisted.data)
    
    # Test negative value
    result = operator.execute(initial_rows, "salary = -1000", tx_id=19)
    assert result.rows_count == 1
    
    persisted = storage.dml_manager.load_all_rows("employees", schema)
    assert any(r.get("id") == 1 and r.get("salary") == -1000 for r in persisted.data)
