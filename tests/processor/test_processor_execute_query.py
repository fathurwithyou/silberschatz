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


def test_execute_recursive_query_execution():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        table_node = QueryTree(
            type=QueryNodeType.TABLE,
            value="users",
            children=[]
        )
        
        result = processor.execute(table_node, tx_id=1)
        
        assert isinstance(result, Rows)
        assert result.rows_count == 3
        assert len(result.data) == 3
        
        first_row = result.data[0]
        assert "users.id" in first_row
        assert "users.name" in first_row
        
    finally:
        cleanup_test_data()


def test_execute_selection_node():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        table_node = QueryTree(type=QueryNodeType.TABLE, value="users", children=[])
        selection_node = QueryTree(
            type=QueryNodeType.SELECTION,
            value="users.age > 28",
            children=[table_node]
        )
        
        result = processor.execute(selection_node, tx_id=1)
        
        assert isinstance(result, Rows)
        assert result.rows_count == 2
        
        for row in result.data:
            assert row["users.age"] > 28
            
    finally:
        cleanup_test_data()


def test_execute_projection_node():
    cleanup_test_data()
    processor = setup_test_environment()
    
    try:
        table_node = QueryTree(type=QueryNodeType.TABLE, value="users", children=[])
        projection_node = QueryTree(
            type=QueryNodeType.PROJECTION,
            value="users.name, users.age",
            children=[table_node]
        )
        
        result = processor.execute(projection_node, tx_id=1)
        
        assert isinstance(result, Rows)
        assert result.rows_count == 3
        
        for row in result.data:
            assert "users.name" in row
            assert "users.age" in row
            
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

if __name__ == "__main__":
    test_execute_query_valid_select()
    test_execute_query_with_where_clause()
    test_execute_query_with_projection()
    test_execute_query_invalid_syntax()
    test_execute_query_nonexistent_table()
    test_execute_query_begin_transaction()
    test_execute_query_commit()
    test_execute_recursive_query_execution()
    test_execute_selection_node()
    test_execute_projection_node()
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
    
    print("All QueryProcessor execute_query tests passed!")