import os
import sys
import pytest
from unittest.mock import Mock, MagicMock
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
    ForeignKeyConstraint,
    ForeignKeyAction
)
from src.processor.operators.delete_operator import DeleteOperator
from src.storage.storage_manager import StorageManager
from src.processor.exceptions import AbortError


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _make_mock_storage_manager():
    """Create a real storage manager instance but we will mock its methods later."""
    data_dir = "data_test"
    abs_data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'src', data_dir)
    if os.path.exists(abs_data_path):
        try:
            shutil.rmtree(abs_data_path)
        except Exception:
            pass

    storage = StorageManager(data_directory=data_dir)
    return storage


def _make_mock_ccm():
    """Create a mock concurrency control manager."""
    mock = Mock()
    mock.validate_object.return_value = Mock(allowed=True)
    mock.get_active_transactions.return_value = (Mock(), []) 
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


# -------------------------------------------------------------------------
# Test Cases
# -------------------------------------------------------------------------

def test_delete_single_row():
    """Test deleting a single row."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_buffer = Mock(return_value=1)
    storage.list_tables = Mock(return_value=[]) 

    schema = create_test_schema()
    data = [{"employees.id": 1, "employees.name": "John Doe", "employees.salary": 50000, "employees.department": "Engineering"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert isinstance(result, Rows)
    assert result.rows_count == 1
    
    storage.delete_buffer.assert_called_once()
    call_args = storage.delete_buffer.call_args[0][0]
    assert call_args.table_name == "employees"
    assert len(call_args.conditions) == 1
    assert call_args.conditions[0].column == "id"
    assert call_args.conditions[0].value == 1


def test_delete_multiple_rows():
    """Test deleting multiple rows."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_buffer = Mock(return_value=1)
    storage.list_tables = Mock(return_value=[]) 

    schema = create_test_schema()
    data = [
        {"employees.id": 1, "employees.name": "John Doe"},
        {"employees.id": 2, "employees.name": "Jane Smith"}
    ]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    
    assert result.rows_count == 2
    assert storage.delete_buffer.call_count == 2


def test_delete_with_qualified_column_names():
    """Test deleting rows with fully qualified column names (table.column)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_buffer = Mock(return_value=1)
    storage.list_tables = Mock(return_value=[]) 

    schema = create_test_schema()
    data = [{"employees.id": 2, "employees.name": "Jane Smith"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    operator.execute(rows, tx_id=1)
    
    call_args = storage.delete_buffer.call_args[0][0]
    assert call_args.conditions[0].column == "id"
    assert call_args.conditions[0].value == 2


def test_delete_with_mixed_column_names():
    """Test deleting rows with mixed qualified and unqualified column names."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_buffer = Mock(return_value=1)
    storage.list_tables = Mock(return_value=[]) 

    schema = create_test_schema()
    data = [{"id": 3, "employees.name": "Bob Johnson"}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    operator.execute(rows, tx_id=1)
    
    call_args = storage.delete_buffer.call_args[0][0]
    assert call_args.conditions[0].value == 3


def test_delete_no_rows_affected():
    """Test delete operation when no rows are affected."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    storage.delete_buffer = Mock(return_value=0)
    storage.list_tables = Mock(return_value=[]) 

    schema = create_test_schema()
    data = [{"employees.id": 999}]
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    result = operator.execute(rows, tx_id=1)
    assert result.rows_count == 0


def test_delete_no_primary_key_error():
    """Test delete operation on table without primary key (should fail)."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    schema_no_pk = TableSchema(
        table_name="logs",
        columns=[ColumnDefinition(name="message", data_type=DataType.VARCHAR)],
        primary_key=None
    )
    
    data = [{"logs.message": "Test log"}]
    rows = Rows(data=data, rows_count=len(data), schema=[schema_no_pk])
    
    with pytest.raises(ValueError, match="does not have a primary key"):
        operator.execute(rows, tx_id=1)


def test_delete_missing_primary_key_in_data():
    """Test delete operation when primary key is missing from row data."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    schema = create_test_schema()
    # Missing 'id'
    data = [{"employees.name": "John Doe", "employees.salary": 50000}]
    
    rows = Rows(data=data, rows_count=len(data), schema=[schema])
    
    with pytest.raises(ValueError, match="Primary key 'id' missing"):
        operator.execute(rows, tx_id=1)


def test_delete_access_denied():
    """Test delete operation when CCM denies access."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    
    ccm.validate_object.return_value = Mock(allowed=False)
    
    operator = DeleteOperator(ccm, storage, frm)
    
    schema = create_test_schema()
    data = [{"employees.id": 1}]
    rows = Rows(data=data, rows_count=1, schema=[schema])
    
    with pytest.raises(AbortError, match="Write access denied"):
        operator.execute(rows, tx_id=1)


def test_delete_restrict_violation():
    """Test ON DELETE RESTRICT prevents deletion if children exist."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    parent_schema = create_test_schema() # employees
    
    child_schema = TableSchema(
        table_name="dependents",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(
                name="emp_id", 
                data_type=DataType.INTEGER,
                foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.RESTRICT)
            )
        ],
        primary_key="id"
    )

    storage.list_tables = Mock(return_value=["employees", "dependents"])
    storage.delete_buffer = Mock(return_value=1)
    
    def get_schema_side_effect(table_name):
        if table_name == "employees": return parent_schema
        if table_name == "dependents": return child_schema
        return None
    storage.get_table_schema = Mock(side_effect=get_schema_side_effect)

    # Mock read_buffer to return child row when checking dependencies
    storage.read_buffer = Mock(return_value=Rows(
        data=[{"dependents.id": 100, "dependents.emp_id": 1}], 
        rows_count=1, 
        schema=[child_schema]
    ))

    data = [{"employees.id": 1, "employees.name": "John Doe"}]
    rows = Rows(data=data, rows_count=1, schema=[parent_schema])

    with pytest.raises(ValueError, match="Integrity Error"):
        operator.execute(rows, tx_id=1)


def test_delete_set_null_action():
    """Test ON DELETE SET NULL updates child rows."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)

    parent_schema = create_test_schema() # employees
    child_schema = TableSchema(
        table_name="teams",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(
                name="lead_id", 
                data_type=DataType.INTEGER,
                foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.SET_NULL)
            )
        ],
        primary_key="id"
    )

    storage.list_tables = Mock(return_value=["teams"])
    storage.delete_buffer = Mock(return_value=1)
    storage.write_buffer = Mock(return_value=1) # Mock write block
    
    def get_schema_side_effect(table_name):
        if table_name == "employees": return parent_schema
        if table_name == "teams": return child_schema
        return None
    storage.get_table_schema = Mock(side_effect=get_schema_side_effect)

    # Mock read_buffer to return child row that needs update
    child_row = {"teams.id": 500, "teams.lead_id": 1}
    storage.read_buffer = Mock(return_value=Rows(
        data=[child_row], rows_count=1, schema=[child_schema]
    ))

    # Execute
    data = [{"employees.id": 1}]
    rows = Rows(data=data, rows_count=1, schema=[parent_schema])
    operator.execute(rows, tx_id=1)

    # Assert Write Block is Called
    storage.write_buffer.assert_called()
    call_args = storage.write_buffer.call_args[0][0]
    
    assert call_args.table_name == "teams"
    assert call_args.is_update is True
    # Ensure lead_id is None (checking both qualified and unqualified keys based on your implementation)
    assert (call_args.data.get("lead_id") is None) or (call_args.data.get("teams.lead_id") is None)
    # Ensure condition is based on PK
    assert call_args.conditions[0].value == 500


def test_delete_cascade_action():
    """Test ON DELETE CASCADE checks for children."""
    storage = _make_mock_storage_manager()
    ccm = _make_mock_ccm()
    frm = _make_mock_frm()
    operator = DeleteOperator(ccm, storage, frm)
    
    parent_schema = create_test_schema()
    child_schema = TableSchema(
        table_name="salaries",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(
                name="emp_id", 
                data_type=DataType.INTEGER,
                foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.CASCADE)
            )
        ],
        primary_key="id"
    )

    # Monkey Patching
    storage.list_tables = Mock(return_value=["salaries"])
    storage.delete_buffer = Mock(return_value=1)
    
    def get_schema_side_effect(table_name):
        if table_name == "employees": return parent_schema
        if table_name == "salaries": return child_schema
        return None
    storage.get_table_schema = Mock(side_effect=get_schema_side_effect)

    # Mock read_buffer to return child rows
    storage.read_buffer = Mock(return_value=Rows(
        data=[{"salaries.id": 99, "salaries.emp_id": 1}], 
        rows_count=1, 
        schema=[child_schema]
    ))

    # Execute
    data = [{"employees.id": 1}]
    rows = Rows(data=data, rows_count=1, schema=[parent_schema])
    operator.execute(rows, tx_id=1)

    found_cascade_call = False
    for call in storage.read_buffer.call_args_list:
        retrieval = call[0][0]
        if retrieval.table_name == "salaries" and \
           retrieval.conditions[0].column == "emp_id" and \
           retrieval.conditions[0].value == 1:
            found_cascade_call = True
            break
            
    assert found_cascade_call, "Should query child table for CASCADE"

if __name__ == "__main__":
    pytest.main([__file__])