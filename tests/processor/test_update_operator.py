import os
import sys
import pytest
from unittest.mock import Mock, MagicMock, patch

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
    
    # mock write_block untuk return number of updated rows
    storage.write_block = Mock(return_value=1)
    
    return storage


def _make_mock_ccm():
    """Create a mock concurrency control manager."""
    ccm = Mock()
    ccm.validate_object = Mock()
    return ccm


def test_update_single_column():
    """Test UPDATE with single column."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"salary": "65000"}
    condition = "id = 2"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    assert storage.write_block.called
    
    # verifikasi struktur DataWrite
    call_args = storage.write_block.call_args[0][0]
    assert isinstance(call_args, DataWrite)
    assert call_args.table_name == "employees"
    assert call_args.is_update == True
    assert call_args.data["salary"] == 65000


def test_update_multiple_columns():
    """Test UPDATE with multiple columns."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {
        "salary": "70000",
        "department": "'Engineering'"
    }
    condition = "id = 1"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi nilai yang diparsing
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["salary"] == 70000
    assert call_args.data["department"] == "Engineering"


def test_update_all_rows_no_where():
    """Test UPDATE without WHERE clause (updates all rows)."""
    storage = _make_mock_storage_manager()
    storage.write_block = Mock(return_value=3)  # simulasi 3 rows diupdate
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"department": "'Marketing'"}
    condition = None # No WHERE clause
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 3
    
    # verifikasi conditions is None
    call_args = storage.write_block.call_args[0][0]
    assert call_args.conditions is None


def test_update_with_where_condition():
    """Test UPDATE with WHERE condition."""
    storage = _make_mock_storage_manager()
    storage.write_block = Mock(return_value=2)
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"salary": "60000"}
    condition = "department = 'Sales'"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 2
    
    # verifikasi conditions diteruskan dengan benar
    call_args = storage.write_block.call_args[0][0]
    assert call_args.conditions == ["department = 'Sales'"]


def test_update_with_null_value():
    """Test UPDATE setting a column to NULL."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"department": "NULL"}
    condition = "id = 1"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi NULL diparsing dengan benar
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["department"] is None


def test_update_nonexistent_table_raises_error():
    """Test UPDATE on non-existent table raises ValueError."""
    storage = _make_mock_storage_manager()
    storage.get_table_schema = Mock(return_value=None)
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    with pytest.raises(ValueError, match="Table .* does not exist"):
        operator.execute("nonexistent", {"col": "value"}, None)


def test_update_nonexistent_column_raises_error():
    """Test UPDATE with non-existent column raises ValueError."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"nonexistent_column": "123"}
    
    with pytest.raises(ValueError, match="Column .* does not exist"):
        operator.execute("employees", assignments, None)


def test_update_with_quoted_string_literal():
    """Test UPDATE with quoted string literal."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"name": "'John Doe'"}
    condition = "id = 1"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi string diunquote
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["name"] == "John Doe"


def test_update_with_double_quoted_string():
    """Test UPDATE with double-quoted string."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"department": '"HR"'}
    condition = "id = 1"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi string diunquote
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["department"] == "HR"


def test_update_integer_type_conversion():
    """Test UPDATE handles integer type conversion correctly."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"salary": "75000"}  # integer as string
    condition = "id = 2"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi konversi tipe
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["salary"] == 75000
    assert isinstance(call_args.data["salary"], int)


def test_update_quoted_integer_for_integer_column():
    """Test UPDATE with quoted integer for integer column."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"salary": "'50000'"}  # quoted integer
    condition = "id = 1"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi konversi ke int
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["salary"] == 50000
    assert isinstance(call_args.data["salary"], int)


def test_parse_value_converts_types_correctly():
    """Test _parse_value method converts values to correct types."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    schema = storage.get_table_schema("employees")
    
    # test INTEGER
    value = operator._parse_value("12345", "id", schema)
    assert value == 12345
    assert isinstance(value, int)
    
    # test VARCHAR with quotes
    value = operator._parse_value("'Alice'", "name", schema)
    assert value == "Alice"
    
    # test NULL
    value = operator._parse_value("NULL", "department", schema)
    assert value is None


def test_parse_value_null_case_insensitive():
    """Test _parse_value handles NULL case-insensitively."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    schema = storage.get_table_schema("employees")
    
    # test various NULL cases
    assert operator._parse_value("NULL", "department", schema) is None
    assert operator._parse_value("null", "department", schema) is None
    assert operator._parse_value("Null", "department", schema) is None


def test_parse_value_invalid_integer_raises_error():
    """Test _parse_value raises error for invalid integer conversion."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    schema = storage.get_table_schema("employees")
    
    # coba set nilai non-numeric ke kolom INTEGER
    with pytest.raises(ValueError, match="Cannot convert .* to INTEGER"):
        operator._parse_value("not_a_number", "salary", schema)


def test_parse_value_invalid_quoted_integer_raises_error():
    """Test _parse_value raises error for invalid quoted integer."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    schema = storage.get_table_schema("employees")
    
    # coba set nilai non-numeric yang di-quote ke kolom INTEGER
    with pytest.raises(ValueError, match="Cannot convert .* to INTEGER"):
        operator._parse_value("'abc'", "salary", schema)


def test_parse_value_unquoted_string_for_varchar():
    """Test _parse_value handles unquoted string for VARCHAR column."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    schema = storage.get_table_schema("employees")
    
    # unquoted string for VARCHAR column
    value = operator._parse_value("Engineering", "department", schema)
    assert value == "Engineering"


def test_parse_value_column_not_found_raises_error():
    """Test _parse_value raises error when column not found in schema."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    schema = storage.get_table_schema("employees")
    
    with pytest.raises(ValueError, match="Column .* not found in schema"):
        operator._parse_value("123", "nonexistent_column", schema)


def test_update_no_rows_affected():
    """Test UPDATE with WHERE condition that matches no rows."""
    storage = _make_mock_storage_manager()
    storage.write_block = Mock(return_value=0)  # no rows updated
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {"salary": "100000"}
    condition = "id = 999"  # non-existent ID
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 0


def test_update_builds_correct_data_write_structure():
    """Test that UPDATE builds correct DataWrite structure."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {
        "salary": "80000",
        "department": "'IT'"
    }
    condition = "id = 2"
    
    operator.execute("employees", assignments, condition)
    
    # verifikasi struktur DataWrite
    call_args = storage.write_block.call_args[0][0]
    assert isinstance(call_args, DataWrite)
    assert call_args.table_name == "employees"
    assert call_args.is_update == True
    assert call_args.data["salary"] == 80000
    assert call_args.data["department"] == "IT"
    assert call_args.conditions == ["id = 2"]


def test_update_with_complex_values():
    """Test UPDATE with various value types in one query."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    assignments = {
        "id": "999",                    # Integer
        "name": "'New Name'",           # Quoted string
        "salary": "85000",              # Integer
        "department": "'Sales'"         # Quoted string
    }
    condition = "id = 1"
    
    updated_count = operator.execute("employees", assignments, condition)
    
    assert updated_count == 1
    
    # verifikasi semua nilai telah diparse dengan benar
    call_args = storage.write_block.call_args[0][0]
    assert call_args.data["id"] == 999
    assert call_args.data["name"] == "New Name"
    assert call_args.data["salary"] == 85000
    assert call_args.data["department"] == "Sales"


# def test_apply_assignments_creates_updated_row():
#     """Test _apply_assignments method creates correct updated row."""
#     storage = _make_mock_storage_manager()
#     ccm = _make_mock_ccm()
#     operator = UpdateOperator(ccm, storage)
    
#     schema = storage.get_table_schema("employees")
    
#     original_row = {
#         "id": 1,
#         "name": "Alice",
#         "salary": 50000,
#         "department": "Sales"
#     }
    
#     assignments = {
#         "salary": "60000",
#         "department": "'Engineering'"
#     }
    
#     updated_row = operator._apply_assignments(original_row, assignments, schema)
    
#     # verifikasi kolom asli tetap terjaga
#     assert updated_row["id"] == 1
#     assert updated_row["name"] == "Alice"
    
#     # verifikasi kolom yang diupdate
#     assert updated_row["salary"] == 60000
#     assert updated_row["department"] == "Engineering"


# Integration-style test
def test_update_integration_flow():
    """Test complete UPDATE flow from start to finish."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    operator = UpdateOperator(ccm, storage)
    
    # Simulate: UPDATE employees SET salary = 80000, department = 'IT' WHERE id = 2
    assignments = {
        "salary": "80000",
        "department": "'IT'"
    }
    condition = "id = 2"
    
    # Execute update
    updated_count = operator.execute("employees", assignments, condition)
    
    # Verify
    assert updated_count == 1
    assert storage.get_table_schema.called
    assert storage.write_block.called
    
    # verifikasi DataWrite dibuat dengan benar
    call_args = storage.write_block.call_args[0][0]
    assert call_args.table_name == "employees"
    assert call_args.is_update == True
    assert call_args.data["salary"] == 80000
    assert call_args.data["department"] == "IT"
    assert call_args.conditions == ["id = 2"]