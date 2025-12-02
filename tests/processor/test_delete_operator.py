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
    DataDeletion,
    Condition,
    ComparisonOperator,
)
from src.processor.operators.delete_operator import DeleteOperator
from src.storage.storage_manager import StorageManager
from src.concurrency.concurrency_manager import ConcurrencyControlManager


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _make_mock_storage_manager():
    """Create a mock storage manager with test data."""
    data_dir = "data_test"
    abs_data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'src', data_dir)
    if os.path.exists(abs_data_path):
        try:
            shutil.rmtree(abs_data_path)
        except Exception:
            pass

    storage = StorageManager(data_directory=data_dir)

    # Create employees table schema
    employees_schema = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="salary", data_type=DataType.INTEGER),
            ColumnDefinition(name="department", data_type=DataType.VARCHAR, max_length=30),
        ],
        primary_key="id",
    )

    try:
        storage.create_table(employees_schema)
    except Exception:
        pass  # Table might already exist

    return storage


def _make_mock_ccm():
    """Create a mock concurrency control manager."""
    mock = Mock()
    # Mock the validate_object method to return allowed=True
    mock.validate_object.return_value = Mock(allowed=True)
    # Mock get_active_transactions to return a tuple where [1] is a list
    mock.get_active_transactions.return_value = (Mock(), [])  # (result, active_transactions_list)
    return mock


def _make_mock_frm():
    """Create a mock failure recovery manager."""
    return Mock()


def create_test_schema():
    """Create a test table schema for employees."""
    columns = [
        ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
        ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
        ColumnDefinition(name="salary", data_type=DataType.INTEGER),
        ColumnDefinition(name="department", data_type=DataType.VARCHAR, max_length=30),
    ]
    return TableSchema(table_name="employees", columns=columns, primary_key="id")


def create_test_data():
    """Create test data for employees table."""
    return [
        {"employees.id": 1, "employees.name": "John Doe", "employees.salary": 50000, "employees.department": "Engineering"},
        {"employees.id": 2, "employees.name": "Jane Smith", "employees.salary": 60000, "employees.department": "Marketing"},
        {"employees.id": 3, "employees.name": "Bob Johnson", "employees.salary": 55000, "employees.department": "Engineering"},
        {"employees.id": 4, "employees.name": "Alice Brown", "employees.salary": 65000, "employees.department": "HR"},
        {"employees.id": 5, "employees.name": "Charlie Wilson", "employees.salary": 70000, "employees.department": "Finance"},
    ]


# -------------------------------------------------------------------------
# Test Cases
# -------------------------------------------------------------------------

def test_delete_single_row():
    """Test deleting a single row."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_block = Mock(return_value=1)

    schema = create_test_schema()
    data = [{"employees.id": 1, "employees.name": "John Doe", "employees.salary": 50000, "employees.department": "Engineering"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert isinstance(result, Rows)
    assert result.rows_count == 1
    assert result.schema == []
    assert result.data == []
    
    storage.delete_block.assert_called_once()
    call_args = storage.delete_block.call_args[0][0]
    assert call_args.table_name == "employees"
    assert len(call_args.conditions) == 1
    assert call_args.conditions[0].column == "id"
    assert call_args.conditions[0].operator == ComparisonOperator.EQ
    assert call_args.conditions[0].value == 1


def test_delete_multiple_rows():
    """Test deleting multiple rows."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_block = Mock(return_value=1)

    schema = create_test_schema()
    data = [
        {"employees.id": 1, "employees.name": "John Doe", "employees.salary": 50000, "employees.department": "Engineering"},
        {"employees.id": 2, "employees.name": "Jane Smith", "employees.salary": 60000, "employees.department": "Marketing"}
    ]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert isinstance(result, Rows)
    assert result.rows_count == 2
    assert result.schema == []
    assert result.data == []
    
    assert storage.delete_block.call_count == 2
    
    first_call = storage.delete_block.call_args_list[0][0][0]
    assert first_call.table_name == "employees"
    assert first_call.conditions[0].value == 1
    
    second_call = storage.delete_block.call_args_list[1][0][0]
    assert second_call.table_name == "employees"
    assert second_call.conditions[0].value == 2


def test_delete_with_qualified_column_names():
    """Test deleting rows with fully qualified column names (table.column)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_block = Mock(return_value=1)

    schema = create_test_schema()
    data = [{"employees.id": 2, "employees.name": "Jane Smith", "employees.salary": 60000, "employees.department": "Marketing"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert result.rows_count == 1
    
    call_args = storage.delete_block.call_args[0][0]
    assert call_args.conditions[0].column == "id"
    assert call_args.conditions[0].value == 2


def test_delete_with_mixed_column_names():
    """Test deleting rows with mixed qualified and unqualified column names."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_block = Mock(return_value=1)

    schema = create_test_schema()
    # Mix of qualified and unqualified names
    data = [{"id": 3, "employees.name": "Bob Johnson", "salary": 55000, "employees.department": "Engineering"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert result.rows_count == 1
    
    # Verify the primary key was found correctly
    call_args = storage.delete_block.call_args[0][0]
    assert call_args.conditions[0].value == 3


def test_delete_no_rows_affected():
    """Test delete operation when no rows are affected."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_block = Mock(return_value=0)

    schema = create_test_schema()
    data = [{"employees.id": 999, "employees.name": "Non Existent", "employees.salary": 0, "employees.department": "None"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert result.rows_count == 0
    storage.delete_block.assert_called_once()


def test_delete_partial_success():
    """Test delete operation with partial success (some deletes fail)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_block = Mock(side_effect=[1, 0, 1])

    schema = create_test_schema()
    data = [
        {"employees.id": 1, "employees.name": "John Doe", "employees.salary": 50000, "employees.department": "Engineering"},
        {"employees.id": 2, "employees.name": "Jane Smith", "employees.salary": 60000, "employees.department": "Marketing"},
        {"employees.id": 3, "employees.name": "Bob Johnson", "employees.salary": 55000, "employees.department": "Engineering"}
    ]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert result.rows_count == 2
    assert storage.delete_block.call_count == 3
    
    calls = storage.delete_block.call_args_list
    assert calls[0][0][0].conditions[0].value == 1
    assert calls[1][0][0].conditions[0].value == 2
    assert calls[2][0][0].conditions[0].value == 3


def test_delete_multiple_tables_error():
    """Test delete operation with multiple table schemas (should fail)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    # Create two different table schemas
    schema1 = create_test_schema()
    schema2 = TableSchema(
        table_name="departments",
        columns=[ColumnDefinition(name="dept_id", data_type=DataType.INTEGER, primary_key=True)],
        primary_key="dept_id"
    )
    
    data = [{"employees.id": 1, "employees.name": "John Doe"}]
    rows = Rows(data=data, rows_count=len(data), schema=[schema1, schema2])
    
    with pytest.raises(ValueError, match="DeleteOperator only supports single table deletions"):
        operator.execute(rows, tx_id=1)


def test_delete_no_primary_key_error():
    """Test delete operation on table without primary key (should fail)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    schema_no_pk = TableSchema(
        table_name="logs",
        columns=[
            ColumnDefinition(name="message", data_type=DataType.VARCHAR),
            ColumnDefinition(name="timestamp", data_type=DataType.VARCHAR),
        ],
        primary_key=None
    )
    
    data = [{"logs.message": "Test log", "logs.timestamp": "2023-01-01"}]
    rows = Rows(data=data, rows_count=len(data), schema=[schema_no_pk])
    
    with pytest.raises(ValueError, match="Table 'logs' does not have a primary key"):
        operator.execute(rows, tx_id=1)


def test_delete_missing_primary_key_in_data():
    """Test delete operation when primary key is missing from row data."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    schema = create_test_schema()
    data = [{"employees.name": "John Doe", "employees.salary": 50000, "employees.department": "Engineering"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    with pytest.raises(ValueError, match="Primary key 'id' missing in row data"):
        operator.execute(rows, tx_id=1)


def test_transform_col_name_method():
    """Test the _transform_col_name method directly."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    row_with_qualified = {
        "employees.id": 1,
        "employees.name": "John Doe",
        "employees.salary": 50000
    }
    
    transformed = operator._transform_col_name(row_with_qualified)
    
    expected = {
        "id": 1,
        "name": "John Doe",
        "salary": 50000
    }
    
    assert transformed == expected

    row_mixed = {
        "employees.id": 1,
        "name": "John Doe",
        "employees.department": "Engineering"
    }
    
    transformed_mixed = operator._transform_col_name(row_mixed)
    
    expected_mixed = {
        "id": 1,
        "name": "John Doe",
        "department": "Engineering"
    }
    
    assert transformed_mixed == expected_mixed

    row_unqualified = {
        "id": 1,
        "name": "John Doe",
        "salary": 50000
    }
    
    transformed_unqualified = operator._transform_col_name(row_unqualified)
    
    assert transformed_unqualified == row_unqualified


def test_transform_col_name_with_nested_dots():
    """Test _transform_col_name with nested dot notation."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    row_nested = {
        "schema.employees.id": 1,
        "schema.employees.name": "John Doe"
    }
    
    transformed = operator._transform_col_name(row_nested)
    
    expected = {
        "id": 1,
        "name": "John Doe"
    }
    
    assert transformed == expected


if __name__ == "__main__":
    pytest.main([__file__])