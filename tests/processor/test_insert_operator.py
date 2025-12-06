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
)
from src.processor.operators.insert_operator import InsertOperator
from src.storage.storage_manager import StorageManager
from src.concurrency.concurrency_manager import ConcurrencyControlManager
from src.failure.failure_recovery_manager import FailureRecoveryManager


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

    # Create users table schema for testing nullable columns
    users_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True, nullable=False),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50, nullable=False),
            ColumnDefinition(name="email", data_type=DataType.VARCHAR, max_length=100, nullable=True),
            ColumnDefinition(name="age", data_type=DataType.INTEGER, nullable=True),
        ],
        primary_key="id",
    )

    try:
        storage.create_table(users_schema)
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


# -------------------------------------------------------------------------
# Test Cases
# -------------------------------------------------------------------------

def test_insert_with_all_columns_specified():
    """Test inserting with all columns specified."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    storage.write_buffer = Mock(return_value=1)

    values_str = "(id, name, salary, department) (1, 'John Doe', 50000, 'Engineering')"
    result = operator.execute("employees", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    assert result.schema == []
    assert result.data == []
    
    storage.write_buffer.assert_called_once()
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.table_name == "employees"
    assert write_call.data == {
        "id": 1,
        "name": "John Doe",
        "salary": 50000,
        "department": "Engineering"
    }
    assert write_call.is_update == False
    assert write_call.conditions is None


def test_insert_with_partial_columns_specified():
    """Test inserting with only some columns specified."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    storage.write_buffer = Mock(return_value=1)

    values_str = "(id, name) (2, 'Jane Smith')"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.table_name == "users"
    assert write_call.data == {
        "id": 2,
        "name": "Jane Smith",
        "email": None,
        "age": None
    }


def test_insert_without_columns_specified():
    """Test inserting without specifying columns (values in schema order)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    storage.write_buffer = Mock(return_value=1)

    values_str = "(3, 'Bob Johnson', 'bob@email.com', 30)"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.table_name == "users"
    assert write_call.data == {
        "id": 3,
        "name": "Bob Johnson", 
        "email": "bob@email.com",
        "age": 30
    }


def test_insert_with_quoted_values():
    """Test inserting with quoted string values."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)
    
    storage.write_buffer = Mock(return_value=1)

    values_str = "(4, 'Alice Brown', 'alice@test.com', 25)"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data["name"] == "Alice Brown"
    assert write_call.data["email"] == "alice@test.com"
    
    storage.write_buffer.reset_mock()

    values_str = '(5, "Charlie Wilson", "charlie@test.com", 35)'
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data["name"] == "Charlie Wilson"
    assert write_call.data["email"] == "charlie@test.com"


def test_insert_with_null_values():
    """Test inserting with NULL values."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    storage.write_buffer = Mock(return_value=1)

    values_str = "(6, 'David Lee', NULL, NULL)"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data == {
        "id": 6,
        "name": "David Lee",
        "email": None,
        "age": None
    }


def test_insert_with_missing_values_for_nullable_columns():
    """Test inserting with missing values for nullable columns."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)
    
    storage.write_buffer = Mock(return_value=1)

    values_str = "(7, 'Eva Garcia')"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data["id"] == 7
    assert write_call.data["name"] == "Eva Garcia"
    assert write_call.data["email"] is None
    assert write_call.data["age"] is None


def test_insert_parse_value_list_with_commas_in_quotes():
    """Test parsing values with commas inside quoted strings."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)
    
    storage.write_buffer = Mock(return_value=1)

    values_str = "(8, 'Smith, John Jr.', 'john.smith@email.com', 40)"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data["name"] == "Smith, John Jr."
    assert write_call.data["email"] == "john.smith@email.com"


def test_insert_parse_value_list_with_escaped_quotes():
    """Test parsing values with escaped quotes."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)
    
    storage.write_buffer = Mock(return_value=1)

    values_str = "(9, 'O''Connor', 'oconnor@email.com', 28)"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data["name"] == "O''Connor"


def test_insert_type_conversion():
    """Test automatic type conversion of values."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)
    
    storage.write_buffer = Mock(return_value=1)

    values_str = "(10, 'Test User', 'test@email.com', '42')"
    result = operator.execute("users", values_str, tx_id=1)

    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    write_call = storage.write_buffer.call_args[0][0]
    assert write_call.data["age"] == 42
    assert isinstance(write_call.data["age"], int)


def test_insert_invalid_table():
    """Test inserting into non-existent table."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    values_str = "(1, 'Test')"
    
    with pytest.raises(ValueError, match="Table 'nonexistent' does not exist"):
        operator.execute("nonexistent", values_str, tx_id=1)


def test_insert_multiple_table_names():
    """Test inserting with multiple table names (should fail)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    values_str = "(1, 'Test')"
    
    with pytest.raises(ValueError, match="InsertOperator only supports inserting into a single table"):
        operator.execute("table1 table2", values_str, tx_id=1)


def test_insert_mismatched_columns_and_values():
    """Test inserting with mismatched number of columns and values."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    # More columns than values
    values_str = "(id, name, email, age) (1, 'Test')"
    
    with pytest.raises(ValueError, match="Number of columns"):
        operator.execute("users", values_str, tx_id=1)


def test_insert_invalid_format_missing_parentheses():
    """Test inserting with invalid format (missing parentheses)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    values_str = "1, 'Test User'"
    
    with pytest.raises(ValueError, match="Invalid format"):
        operator.execute("users", values_str, tx_id=1)


def test_insert_invalid_type_conversion():
    """Test inserting with invalid type conversion."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    # Try to insert non-integer value for integer column
    values_str = "(id, name) ('not_a_number', 'Test User')"
    
    with pytest.raises(ValueError, match="Cannot convert"):
        operator.execute("users", values_str, tx_id=1)


def test_parse_value_list_empty_values():
    """Test parsing empty values."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    # Test with empty string values
    result = operator._parse_value_list("'', 'Test', ''")
    assert result == ["''", "'Test'", "''"]


def test_parse_value_with_different_data_types():
    """Test _parse_value method with different data types."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = InsertOperator(ccm, storage, frm)

    # Test INTEGER conversion
    result = operator._parse_value("42", DataType.INTEGER)
    assert result == 42

    # Test FLOAT conversion
    result = operator._parse_value("3.14", DataType.FLOAT)
    assert result == 3.14

    # Test VARCHAR
    result = operator._parse_value("'test string'", DataType.VARCHAR)
    assert result == "test string"

    # Test NULL
    result = operator._parse_value("NULL", DataType.INTEGER)
    assert result is None


if __name__ == "__main__":
    pytest.main([__file__])