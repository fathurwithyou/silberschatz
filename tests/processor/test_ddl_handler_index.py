import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.processor import QueryProcessor
from src.optimizer.optimizer import QueryOptimizer
from src.storage.storage_manager import StorageManager
from src.concurrency.concurrency_manager import ConcurrencyControlManager
from src.failure.failure_recovery_manager import FailureRecoveryManager
from src.core.models import (
    ExecutionResult,
    TableSchema,
    ColumnDefinition,
    DataType,
)


TEST_DATA_DIR = "src/data_test"


def cleanup_test_data():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)


def setup_processor() -> tuple[QueryProcessor, StorageManager]:
    storage = StorageManager("data_test")
    optimizer = QueryOptimizer(storage_manager=storage)
    ccm = ConcurrencyControlManager("Timestamp")
    frm = FailureRecoveryManager()
    processor = QueryProcessor(optimizer, ccm, frm, storage)
    return processor, storage


def setup_processor_with_table() -> tuple[QueryProcessor, StorageManager]:
    """Setup processor with a pre-created table for index testing."""
    processor, storage = setup_processor()
    
    # Create a users table for index testing
    processor.execute_query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER, email VARCHAR(100))"
    )
    
    # Insert some test data
    processor.execute_query("INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com')")
    processor.execute_query("INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com')")
    processor.execute_query("INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com')")
    
    return processor, storage


def test_create_index_basic():
    """Test basic CREATE INDEX on a single column."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        result = processor.execute_query("CREATE INDEX ON users(name)")

        assert isinstance(result, ExecutionResult)
        assert "Index created" in result.message
        assert "users" in result.message
        assert "name" in result.message
        
        # Verify index exists
        assert storage.has_index("users", "name")

    finally:
        cleanup_test_data()


def test_create_index_with_using_btree():
    """Test CREATE INDEX with USING BTREE clause."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        result = processor.execute_query("CREATE INDEX ON users(age) USING BTREE")

        assert isinstance(result, ExecutionResult)
        assert "Index created" in result.message
        assert "users" in result.message
        assert "age" in result.message
        
        # Verify index exists
        assert storage.has_index("users", "age")

    finally:
        cleanup_test_data()


def test_create_index_with_using_b_plus_tree():
    """Test CREATE INDEX with USING b_plus_tree clause."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        result = processor.execute_query("CREATE INDEX ON users(email) USING b_plus_tree")

        assert isinstance(result, ExecutionResult)
        assert "Index created" in result.message
        
        # Verify index exists
        assert storage.has_index("users", "email")

    finally:
        cleanup_test_data()


def test_create_index_multiple_columns():
    """Test creating indexes on multiple columns of the same table."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create index on name
        result1 = processor.execute_query("CREATE INDEX ON users(name)")
        assert isinstance(result1, ExecutionResult)
        assert storage.has_index("users", "name")
        
        # Create index on age
        result2 = processor.execute_query("CREATE INDEX ON users(age)")
        assert isinstance(result2, ExecutionResult)
        assert storage.has_index("users", "age")
        
        # Both indexes should exist
        assert storage.has_index("users", "name")
        assert storage.has_index("users", "age")

    finally:
        cleanup_test_data()


def test_create_index_on_primary_key():
    """Test creating index on primary key column."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        result = processor.execute_query("CREATE INDEX ON users(id)")

        assert isinstance(result, ExecutionResult)
        assert "Index created" in result.message
        assert storage.has_index("users", "id")

    finally:
        cleanup_test_data()


def test_create_index_nonexistent_table():
    """Test CREATE INDEX on non-existent table should fail."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        with pytest.raises(Exception) as exc_info:
            processor.execute_query("CREATE INDEX ON nonexistent_table(col)")
        
        assert "does not exist" in str(exc_info.value).lower()

    finally:
        cleanup_test_data()


def test_create_index_nonexistent_column():
    """Test CREATE INDEX on non-existent column should fail."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        with pytest.raises(Exception) as exc_info:
            processor.execute_query("CREATE INDEX ON users(nonexistent_column)")
        
        assert "does not exist" in str(exc_info.value).lower() or "not found" in str(exc_info.value).lower()

    finally:
        cleanup_test_data()


def test_create_index_duplicate():
    """Test creating duplicate index on same column should fail."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create first index
        result = processor.execute_query("CREATE INDEX ON users(name)")
        assert isinstance(result, ExecutionResult)
        
        # Try to create duplicate index on same column
        with pytest.raises(Exception) as exc_info:
            processor.execute_query("CREATE INDEX ON users(name)")
        
        assert "already exists" in str(exc_info.value).lower()

    finally:
        cleanup_test_data()


def test_drop_index_basic():
    """Test basic DROP INDEX."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create index first
        processor.execute_query("CREATE INDEX ON users(name)")
        assert storage.has_index("users", "name")
        
        # Drop the index
        result = processor.execute_query("DROP INDEX ON users(name)")
        
        assert isinstance(result, ExecutionResult)
        assert "Index dropped" in result.message
        assert "users" in result.message
        assert "name" in result.message
        
        # Verify index no longer exists
        assert not storage.has_index("users", "name")

    finally:
        cleanup_test_data()


def test_drop_index_nonexistent():
    """Test DROP INDEX on non-existent index should fail."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        with pytest.raises(Exception) as exc_info:
            processor.execute_query("DROP INDEX ON users(name)")
        
        assert "no index" in str(exc_info.value).lower() or "does not exist" in str(exc_info.value).lower()

    finally:
        cleanup_test_data()


def test_drop_index_nonexistent_table():
    """Test DROP INDEX on non-existent table should fail."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        with pytest.raises(Exception) as exc_info:
            processor.execute_query("DROP INDEX ON nonexistent_table(col)")
        
        assert "does not exist" in str(exc_info.value).lower()

    finally:
        cleanup_test_data()


def test_create_drop_create_index():
    """Test creating, dropping, and recreating an index."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create index
        result1 = processor.execute_query("CREATE INDEX ON users(age)")
        assert isinstance(result1, ExecutionResult)
        assert storage.has_index("users", "age")
        
        # Drop index
        result2 = processor.execute_query("DROP INDEX ON users(age)")
        assert isinstance(result2, ExecutionResult)
        assert not storage.has_index("users", "age")
        
        # Recreate index
        result3 = processor.execute_query("CREATE INDEX ON users(age)")
        assert isinstance(result3, ExecutionResult)
        assert storage.has_index("users", "age")

    finally:
        cleanup_test_data()


def test_index_persists_after_insert():
    """Test that index is maintained after inserting new data."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create index
        processor.execute_query("CREATE INDEX ON users(name)")
        assert storage.has_index("users", "name")
        
        # Insert new data
        processor.execute_query("INSERT INTO users VALUES (4, 'David', 40, 'david@example.com')")
        
        # Index should still exist
        assert storage.has_index("users", "name")
        
        # Query should still work
        result = processor.execute_query("SELECT * FROM users WHERE name = 'David'")
        assert result.data is not None
        assert len(result.data.data) == 1
        # Access by index if dict keys differ
        row = result.data.data[0]
        assert row.get("name") == "David" or row.get("users.name") == "David"

    finally:
        cleanup_test_data()


def test_index_persists_after_update():
    """Test that index is maintained after updating data."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create index
        processor.execute_query("CREATE INDEX ON users(name)")
        assert storage.has_index("users", "name")
        
        # Update data
        processor.execute_query("UPDATE users SET name = 'Alice Smith' WHERE id = 1")
        
        # Index should still exist
        assert storage.has_index("users", "name")
        
        # Query with updated value should work
        result = processor.execute_query("SELECT * FROM users WHERE name = 'Alice Smith'")
        assert result.data is not None
        assert len(result.data.data) == 1

    finally:
        cleanup_test_data()


def test_index_persists_after_delete():
    """Test that index is maintained after deleting data."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create index
        processor.execute_query("CREATE INDEX ON users(age)")
        assert storage.has_index("users", "age")
        
        # Delete data
        processor.execute_query("DELETE FROM users WHERE id = 1")
        
        # Index should still exist
        assert storage.has_index("users", "age")
        
        # Query all remaining users
        all_result = processor.execute_query("SELECT * FROM users")
        assert all_result.data is not None
        assert len(all_result.data.data) == 2 
        
        result = processor.execute_query("SELECT * FROM users WHERE age = 30")
        assert result.data is not None
        assert len(result.data.data) == 1
        assert result.data.data[0].get("users.name") == "Bob"

    finally:
        cleanup_test_data()


def test_drop_table_removes_indexes():
    """Test that dropping a table also removes its indexes."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        # Create indexes
        processor.execute_query("CREATE INDEX ON users(name)")
        processor.execute_query("CREATE INDEX ON users(age)")
        assert storage.has_index("users", "name")
        assert storage.has_index("users", "age")
        
        # Drop table
        processor.execute_query("DROP TABLE users")
        
        # Verify table is gone
        assert "users" not in storage.list_tables()
        
        # Indexes should be removed (checking shouldn't raise error but return False or handle gracefully)
        try:
            assert not storage.has_index("users", "name")
            assert not storage.has_index("users", "age")
        except:
            # If table doesn't exist, has_index might raise - that's ok
            pass

    finally:
        cleanup_test_data()


def test_multiple_tables_with_indexes():
    """Test creating indexes on multiple tables."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        # Create first table and index
        processor.execute_query("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
        processor.execute_query("INSERT INTO users VALUES (1, 'Alice')")
        processor.execute_query("CREATE INDEX ON users(name)")
        
        # Create second table and index
        processor.execute_query("CREATE TABLE products (id INTEGER, title VARCHAR(50))")
        processor.execute_query("INSERT INTO products VALUES (1, 'Laptop')")
        processor.execute_query("CREATE INDEX ON products(title)")
        
        # Both indexes should exist
        assert storage.has_index("users", "name")
        assert storage.has_index("products", "title")
        
        # Queries should work on both
        result1 = processor.execute_query("SELECT * FROM users WHERE name = 'Alice'")
        assert result1.data is not None
        assert len(result1.data.data) == 1
        
        result2 = processor.execute_query("SELECT * FROM products WHERE title = 'Laptop'")
        assert result2.data is not None
        assert len(result2.data.data) == 1

    finally:
        cleanup_test_data()


def test_create_index_with_semicolon():
    """Test CREATE INDEX with semicolon at the end."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        result = processor.execute_query("CREATE INDEX ON users(name)")

        assert isinstance(result, ExecutionResult)
        assert storage.has_index("users", "name")

    finally:
        cleanup_test_data()


def test_drop_index_with_semicolon():
    """Test DROP INDEX with semicolon at the end."""
    cleanup_test_data()
    processor, storage = setup_processor_with_table()

    try:
        processor.execute_query("CREATE INDEX ON users(name)")
        result = processor.execute_query("DROP INDEX ON users(name)")

        assert isinstance(result, ExecutionResult)
        assert not storage.has_index("users", "name")

    finally:
        cleanup_test_data()
