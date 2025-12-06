import sys
import os
import shutil

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.operators.scan_operator import ScanOperator
from src.core.models.result import Rows
from src.core.models.storage import TableSchema, ColumnDefinition, DataType
from src.core.models import DataRetrieval
from src.storage.storage_manager import StorageManager
from src.concurrency.concurrency_manager import ConcurrencyControlManager


def _make_mock_ccm():
    from unittest.mock import Mock
    from src.core.models.response import Response
    
    mock_ccm = Mock()
    mock_ccm.validate_object.return_value = Response(allowed=True, transaction_id=1)
    mock_ccm.get_active_transactions.return_value = (None, [1, 2, 3])
    return mock_ccm


def cleanup_test_data():
    """Clean up test data directory."""
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def setup_test_storage():
    storage_manager = StorageManager("data_test")
    
    schema = TableSchema(
        table_name="users",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
        ],
        primary_key="id"
    )
    
    storage_manager.create_table(schema)
    
    test_rows = Rows(
        data=[
            {"id": 1, "name": "John", "age": 25},
            {"id": 2, "name": "Jane", "age": 30},
            {"id": 3, "name": "Bob", "age": 35}
        ],
        rows_count=3
    )
    
    storage_manager.dml_manager.save_all_rows("users", test_rows, schema)
    
    return storage_manager


def setup_employees_table():
    storage_manager = StorageManager("data_test")
    
    schema = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="first_name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="last_name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="email", data_type=DataType.VARCHAR, max_length=100),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
            ColumnDefinition(name="salary", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )
    
    storage_manager.create_table(schema)
    
    test_rows = Rows(
        data=[
            {
                "id": 1, 
                "first_name": "John", 
                "last_name": "Doe",
                "email": "john.doe@example.com",
                "age": 25, 
                "salary": 50000.0
            },
            {
                "id": 2, 
                "first_name": "Jane", 
                "last_name": "Smith",
                "email": "jane.smith@example.com", 
                "age": 30, 
                "salary": 60000.0
            }
        ],
        rows_count=2
    )
    
    storage_manager.dml_manager.save_all_rows("employees", test_rows, schema)
    
    return storage_manager


def setup_empty_table():
    storage_manager = StorageManager("data_test")
    
    schema = TableSchema(
        table_name="empty_table",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
        ],
        primary_key="id"
    )
    
    storage_manager.create_table(schema)
    
    return storage_manager


def test_scan_basic_table():
    cleanup_test_data()
    storage_manager = setup_test_storage()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        result = operator.execute("users", tx_id=1)
        
        assert result.rows_count == 3
        assert len(result.data) == 3
        assert len(result.schema) == 1
        assert result.schema[0].table_name == "users"
        
        expected_first_row = {"users.id": 1, "users.name": "John", "users.age": 25}
        assert result.data[0] == expected_first_row
        
    finally:
        cleanup_test_data()


def test_scan_table_with_alias():
    cleanup_test_data()
    storage_manager = setup_test_storage()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        result = operator.execute("users AS u", tx_id=1)
        
        assert result.schema[0].table_name == "u"
        
        expected_first_row = {"u.id": 1, "u.name": "John", "u.age": 25}
        assert result.data[0] == expected_first_row
        
    finally:
        cleanup_test_data()


def test_scan_table_not_found():
    cleanup_test_data()
    storage_manager = setup_test_storage()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        
        try:
            operator.execute("nonexistent_table", tx_id=1)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "does not exist" in str(e)
            assert "nonexistent_table" in str(e)
            
    finally:
        cleanup_test_data()


def test_scan_empty_table():
    cleanup_test_data()
    storage_manager = setup_empty_table()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        result = operator.execute("empty_table", tx_id=1)
        
        assert result.rows_count == 0
        assert len(result.data) == 0
        assert len(result.schema) == 1
        assert result.schema[0].table_name == "empty_table"
        
    finally:
        cleanup_test_data()


def test_parse_table_name_and_alias():
    cleanup_test_data()
    storage_manager = setup_test_storage()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        
        table_name, alias = operator._parse_table_name_and_alias("users")
        assert table_name == "users"
        assert alias == "users"
        
        table_name, alias = operator._parse_table_name_and_alias("users AS u")
        assert table_name == "users"
        assert alias == "u"
        
        table_name, alias = operator._parse_table_name_and_alias("users u")
        assert table_name == "users"
        assert alias == "users"
        
    finally:
        cleanup_test_data()


def test_transform_rows():
    cleanup_test_data()
    storage_manager = setup_test_storage()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        
        original_rows = [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"}
        ]
        
        transformed = operator._transform_rows(original_rows, "users")
        
        expected = [
            {"users.id": 1, "users.name": "John"},
            {"users.id": 2, "users.name": "Jane"}
        ]
        
        assert transformed == expected
        
        transformed_alias = operator._transform_rows(original_rows, "u")
        
        expected_alias = [
            {"u.id": 1, "u.name": "John"},
            {"u.id": 2, "u.name": "Jane"}
        ]
        
        assert transformed_alias == expected_alias
        
    finally:
        cleanup_test_data()


def test_scan_with_multiple_columns():
    cleanup_test_data()
    storage_manager = setup_employees_table()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        result = operator.execute("employees AS e", tx_id=1)
        
        expected_first_row = {
            "e.id": 1,
            "e.first_name": "John",
            "e.last_name": "Doe",
            "e.email": "john.doe@example.com",
            "e.age": 25,
            "e.salary": 50000.0
        }
        
        assert result.data[0] == expected_first_row
        assert result.rows_count == 2
        assert result.schema[0].table_name == "e"
        
    finally:
        cleanup_test_data()


def test_scan_integer_conversion():
    cleanup_test_data()
    storage_manager = setup_test_storage()
    ccm = _make_mock_ccm()
    
    try:
        operator = ScanOperator(ccm, storage_manager)
        result = operator.execute("users", tx_id=1)
        
        for row in result.data:
            assert isinstance(row["users.id"], int)
            assert isinstance(row["users.age"], int)
            assert isinstance(row["users.name"], str)
            
    finally:
        cleanup_test_data()


if __name__ == "__main__":
    cleanup_test_data()  # Clean up before starting
    
    try:
        test_scan_basic_table()
        test_scan_table_with_alias()
        test_scan_table_not_found()
        test_scan_empty_table()
        test_parse_table_name_and_alias()
        test_transform_rows()
        test_scan_with_multiple_columns()
        test_scan_integer_conversion()
        
        print("All scan operator tests passed!")
    finally:
        cleanup_test_data()  # Final cleanup