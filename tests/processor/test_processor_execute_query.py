import sys
import os
import shutil

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.processor import QueryProcessor
from src.optimizer.optimizer import QueryOptimizer
from src.storage.storage_manager import StorageManager
from src.concurrency.concurrency_manager import ConcurrencyControlManager
from src.failure.failure_recovery_manager import FailureRecoveryManager
from src.core.models.result import ExecutionResult, Rows
from src.core.models.query import QueryTree, QueryNodeType
from src.core.models.storage import TableSchema, ColumnDefinition, DataType


def cleanup_test_data():
    """Clean up test data directory."""
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def setup_test_environment():
    storage_manager = StorageManager("data_test")
    optimizer = QueryOptimizer(storage_manager=storage_manager)
    ccm = ConcurrencyControlManager("Timestamp")
    frm = FailureRecoveryManager()
    
    schema = TableSchema(
        table_name="users",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
            ColumnDefinition(name="salary", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )
    
    storage_manager.create_table(schema)
    
    test_rows = Rows(
        data=[
            {"id": 1, "name": "John", "age": 25, "salary": 50000.0},
            {"id": 2, "name": "Jane", "age": 30, "salary": 60000.0},
            {"id": 3, "name": "Bob", "age": 35, "salary": 70000.0}
        ],
        rows_count=3
    )
    
    storage_manager.dml_manager.save_all_rows("users", test_rows, schema)
    
    processor = QueryProcessor(optimizer, ccm, frm, storage_manager)
    return processor


def test_execute_query_valid_select():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users")
        
        assert isinstance(result, ExecutionResult)
        assert result.data is not None
        assert isinstance(result.data, Rows)
        assert result.data.rows_count == 3
        assert len(result.data.data) == 3
        assert result.message is not None
        assert result.query == "SELECT * FROM users"
        
        first_row = result.data.data[0]
        assert "users.id" in first_row
        assert "users.name" in first_row
        assert "users.age" in first_row
        assert "users.salary" in first_row
        
    finally:
        cleanup_test_data()


def test_execute_query_with_where_clause():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users WHERE users.age > 28")
        
        assert result.data is not None
        assert result.data.rows_count == 2
        ages = [row["users.age"] for row in result.data.data]
        assert all(age > 28 for age in ages)
        
    finally:
        cleanup_test_data()


def test_execute_query_with_projection():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT users.name, users.age FROM users")
        
        assert result.data is not None
        assert result.data.rows_count == 3
        for row in result.data.data:
            assert "users.name" in row
            assert "users.age" in row
            
    finally:
        cleanup_test_data()


def test_execute_query_invalid_syntax():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        try:
            processor.execute_query("SELECT FROM users")
            assert False, "Should have raised SyntaxError"
        except SyntaxError as e:
            assert "error" in str(e).lower()
            
    finally:
        cleanup_test_data()


def test_execute_query_nonexistent_table():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        try:
            processor.execute_query("SELECT * FROM nonexistent_table")
            assert False, "Should have raised an exception"
        except Exception as e:
            assert "does not exist" in str(e) or "not found" in str(e).lower()
            
    finally:
        cleanup_test_data()


def test_execute_query_begin_transaction():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("BEGIN TRANSACTION")
        
        assert isinstance(result, ExecutionResult)
        assert result.transaction_id is not None
        assert result.message is not None
        
    finally:
        cleanup_test_data()


def test_execute_query_commit():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        begin_result = processor.execute_query("BEGIN TRANSACTION")
        tx_id = begin_result.transaction_id
        
        processor.transaction_id = tx_id
        
        result = processor.execute_query("COMMIT")
        
        assert isinstance(result, ExecutionResult)
        assert result.message is not None
        
    finally:
        cleanup_test_data()

def test_query_routing():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users")
        assert isinstance(result, ExecutionResult)
        
        result = processor.execute_query("BEGIN TRANSACTION")
        assert isinstance(result, ExecutionResult)
        
    finally:
        cleanup_test_data()


def test_whitespace_normalization():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result1 = processor.execute_query("  SELECT   *   FROM   users  ")
        result2 = processor.execute_query("SELECT * FROM users")
        
        assert result1.data is not None
        assert result2.data is not None
        assert result1.data.rows_count == result2.data.rows_count
        assert len(result1.data.data) == len(result2.data.data)
        
    finally:
        cleanup_test_data()


def test_complex_query():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT users.name FROM users WHERE users.salary > 55000")
        
        assert result.data is not None
        assert result.data.rows_count == 2
        for row in result.data.data:
            assert "users.name" in row
            
    finally:
        cleanup_test_data()


def test_complex_query_with_multiple_conditions():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users WHERE users.age > 25 AND users.salary > 55000")
        
        assert result.data is not None
        assert result.data.rows_count == 2
        for row in result.data.data:
            assert row["users.age"] > 25
            assert row["users.salary"] > 55000
            
    finally:
        cleanup_test_data()


def test_complex_query_with_or_conditions():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT users.name FROM users WHERE users.age < 26 OR users.salary > 65000")
        
        assert result.data is not None
        assert result.data.rows_count == 2
        names = [row["users.name"] for row in result.data.data]
        assert "John" in names
        assert "Bob" in names
            
    finally:
        cleanup_test_data()


def test_complex_query_with_parentheses():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users WHERE (users.age > 30 OR users.salary < 55000) AND users.name != 'John'")
        
        assert result.data is not None
        assert result.data.rows_count == 1
        assert result.data.data[0]["users.name"] == "Bob"
            
    finally:
        cleanup_test_data()


def test_complex_query_multiple_projections():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT users.id, users.name, users.age FROM users WHERE users.age >= 30")
        
        assert result.data is not None
        assert result.data.rows_count == 2
        
        for row in result.data.data:
            assert "users.id" in row
            assert "users.name" in row
            assert "users.age" in row
            assert "users.salary" not in row
            assert row["users.age"] >= 30
            
    finally:
        cleanup_test_data()


def test_complex_query_nested_conditions():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT users.name FROM users WHERE ((users.age > 25 AND users.salary > 50000) OR users.age < 26) AND users.name != 'Jane'")
        
        assert result.data is not None
        assert result.data.rows_count == 2
        names = [row["users.name"] for row in result.data.data]
        assert "John" in names
        assert "Bob" in names
        assert "Jane" not in names
            
    finally:
        cleanup_test_data()


def test_complex_query_boundary_values():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users WHERE users.age >= 25 AND users.age <= 35 AND users.salary >= 50000 AND users.salary <= 70000")
        
        assert result.data is not None
        assert result.data.rows_count == 3
            
    finally:
        cleanup_test_data()


def test_complex_query_with_wildcard_and_conditions():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("SELECT * FROM users WHERE (users.age > 20 AND users.salary > 45000) OR users.name = 'Bob'")
        
        assert result.data is not None
        assert result.data.rows_count == 3
        
        for row in result.data.data:
            assert "users.id" in row
            assert "users.name" in row
            assert "users.age" in row
            assert "users.salary" in row
            
    finally:
        cleanup_test_data()

def test_update_query():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("UPDATE users SET salary = 80000 WHERE id = 2")
        
        assert isinstance(result, ExecutionResult)
        assert result.message is not None
        
        select_result = processor.execute_query("SELECT salary FROM users WHERE id = 2")
        assert select_result.data is not None
        assert select_result.data.rows_count == 1
        assert select_result.data.data[0]["users.salary"] == 80000.0
        
    finally:
        cleanup_test_data()


def test_execute_insert_query_all_columns():
    """Test INSERT query with all columns specified."""
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("INSERT INTO users (id, name, age, salary) VALUES (4, 'Alice', 28, 55000.0)")
        
        assert isinstance(result, ExecutionResult)
        assert result.data is not None
        assert result.data.rows_count == 1
        assert "executed successfully" in result.message.lower() or "insert" in result.message.lower()
        
        select_result = processor.execute_query("SELECT * FROM users WHERE users.id = 4")
        assert select_result.data is not None
        assert select_result.data.rows_count == 1
        new_row = select_result.data.data[0]
        assert new_row["users.id"] == 4
        assert new_row["users.name"] == "Alice"
        assert new_row["users.age"] == 28
        assert new_row["users.salary"] == 55000.0
        
    finally:
        cleanup_test_data()


def test_execute_insert_query_partial_columns():
    """Test INSERT query with only some columns specified."""
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("INSERT INTO users (id, name) VALUES (5, 'Charlie')")
        
        assert isinstance(result, ExecutionResult)
        assert result.data is not None
        assert result.data.rows_count == 1
        
        select_result = processor.execute_query("SELECT * FROM users WHERE users.id = 5")
        assert select_result.data is not None
        assert select_result.data.rows_count == 1
        new_row = select_result.data.data[0]
        assert new_row["users.id"] == 5
        assert new_row["users.name"] == "Charlie"
        assert new_row["users.age"] is None
        assert new_row["users.salary"] is None
        
    finally:
        cleanup_test_data()


def test_execute_insert_query_no_columns_specified():
    """Test INSERT query without specifying columns (values in schema order)."""
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("INSERT INTO users VALUES (6, 'Diana', 32, 62000.0)")
        
        assert isinstance(result, ExecutionResult)
        assert result.data is not None
        assert result.data.rows_count == 1
        
        select_result = processor.execute_query("SELECT * FROM users WHERE users.id = 6")
        assert select_result.data is not None
        assert select_result.data.rows_count == 1
        new_row = select_result.data.data[0]
        assert new_row["users.id"] == 6
        assert new_row["users.name"] == "Diana"
        assert new_row["users.age"] == 32
        assert new_row["users.salary"] == 62000.0
        
    finally:
        cleanup_test_data()


def test_execute_insert_query_with_null_values():
    """Test INSERT query with explicit NULL values."""
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        result = processor.execute_query("INSERT INTO users (id, name, age, salary) VALUES (7, 'Eve', NULL, NULL)")
        
        assert isinstance(result, ExecutionResult)
        assert result.data is not None
        assert result.data.rows_count == 1
        
        select_result = processor.execute_query("SELECT * FROM users WHERE users.id = 7")
        assert select_result.data is not None
        assert select_result.data.rows_count == 1
        new_row = select_result.data.data[0]
        assert new_row["users.id"] == 7
        assert new_row["users.name"] == "Eve"
        assert new_row["users.age"] is None
        assert new_row["users.salary"] is None
        
    finally:
        cleanup_test_data()


def test_execute_delete_query():
    """Test DELETE query to remove specific rows."""
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        initial_result = processor.execute_query("SELECT * FROM users")
        assert initial_result.data is not None
        assert initial_result.data.rows_count == 3
        
        delete_result = processor.execute_query("DELETE FROM users WHERE users.id = 2")
        
        assert isinstance(delete_result, ExecutionResult)
        assert delete_result.data is not None
        assert delete_result.data.rows_count == 1
        assert "delete successful" in delete_result.message.lower() or "executed successfully" in delete_result.message.lower()
        
        remaining_result = processor.execute_query("SELECT * FROM users")
        assert remaining_result.data is not None
        assert remaining_result.data.rows_count == 2
        
        jane_check = processor.execute_query("SELECT * FROM users WHERE users.id = 2")
        assert jane_check.data is not None
        assert jane_check.data.rows_count == 0
        
        remaining_ids = [row["users.id"] for row in remaining_result.data.data]
        assert set(remaining_ids) == {1, 3}
        
    finally:
        cleanup_test_data()


def test_execute_delete_query_with_conditions():
    """Test DELETE query with complex WHERE conditions."""
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        delete_result = processor.execute_query("DELETE FROM users WHERE users.salary > 55000")
        
        assert isinstance(delete_result, ExecutionResult)
        assert delete_result.data is not None
        assert delete_result.data.rows_count == 2
        
        remaining_result = processor.execute_query("SELECT * FROM users")
        assert remaining_result.data is not None
        assert remaining_result.data.rows_count == 1
        
        remaining_user = remaining_result.data.data[0]
        assert remaining_user["users.id"] == 1
        assert remaining_user["users.name"] == "John"
        assert remaining_user["users.salary"] == 50000.0
        
    finally:
        cleanup_test_data()


if __name__ == "__main__":
    test_execute_query_valid_select()
    test_execute_query_with_where_clause()
    test_execute_query_with_projection()
    test_execute_query_invalid_syntax()
    test_execute_query_nonexistent_table()
    test_execute_query_begin_transaction()
    test_execute_query_commit()
    test_query_routing()
    test_whitespace_normalization()
    test_update_query()
    test_complex_query()
    test_complex_query_with_multiple_conditions()
    test_complex_query_with_or_conditions()
    test_complex_query_with_parentheses()
    test_complex_query_multiple_projections()
    test_complex_query_nested_conditions()
    test_complex_query_boundary_values()
    test_complex_query_with_wildcard_and_conditions()
    test_execute_insert_query_all_columns()
    test_execute_insert_query_partial_columns()
    test_execute_insert_query_no_columns_specified()
    test_execute_insert_query_with_null_values()
    test_execute_delete_query()
    test_execute_delete_query_with_conditions()
    
    print("All QueryProcessor execute_query tests passed!")