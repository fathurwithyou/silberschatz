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
    ForeignKeyConstraint,
    ForeignKeyAction,
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


def setup_processor_with_existing_table() -> tuple[QueryProcessor, StorageManager]:
    processor, storage = setup_processor()
    
    # Create a departments table for foreign key testing
    departments = TableSchema(
        table_name="departments",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=100),
            ColumnDefinition(name="budget", data_type=DataType.FLOAT),
        ],
        primary_key="id",
    )
    storage.create_table(departments)
    
    return processor, storage


def test_create_table_basic():
    """Test basic CREATE TABLE with simple columns."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        result = processor.execute_query(
            "CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER)"
        )

        assert isinstance(result, ExecutionResult)
        assert "users" in result.message
        assert "created" in result.message.lower()
        assert "users" in storage.list_tables()
        
        # Verify table schema
        schema = storage.get_table_schema("users")
        assert schema is not None
        assert schema.table_name == "users"
        assert len(schema.columns) == 3
        
        # Check column details
        columns_by_name = {col.name: col for col in schema.columns}
        assert "id" in columns_by_name
        assert columns_by_name["id"].data_type == DataType.INTEGER
        assert "name" in columns_by_name
        assert columns_by_name["name"].data_type == DataType.VARCHAR
        assert columns_by_name["name"].max_length == 50
        assert "age" in columns_by_name
        assert columns_by_name["age"].data_type == DataType.INTEGER

    finally:
        cleanup_test_data()


def test_create_table_with_primary_key():
    """Test CREATE TABLE with PRIMARY KEY constraint."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        result = processor.execute_query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))"
        )

        assert isinstance(result, ExecutionResult)
        assert "users" in storage.list_tables()
        
        schema = storage.get_table_schema("users")
        assert schema is not None
        assert schema.primary_key == "id"
        
        # Verify primary key column properties
        id_column = next(col for col in schema.columns if col.name == "id")
        assert id_column.primary_key is True
        assert id_column.nullable is False  # PRIMARY KEY implies NOT NULL

    finally:
        cleanup_test_data()


def test_create_table_with_not_null():
    """Test CREATE TABLE with NOT NULL constraints."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        result = processor.execute_query(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, price FLOAT)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("products")
        assert schema is not None
        
        columns_by_name = {col.name: col for col in schema.columns}
        assert columns_by_name["id"].nullable is False  # PRIMARY KEY
        assert columns_by_name["name"].nullable is False  # NOT NULL
        assert columns_by_name["price"].nullable is True  # No constraint specified

    finally:
        cleanup_test_data()


def test_create_table_with_foreign_key():
    """Test CREATE TABLE with foreign key constraint."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name VARCHAR(100), dept_id INTEGER REFERENCES departments(id))"
        )

        assert isinstance(result, ExecutionResult)
        assert "employees" in storage.list_tables()
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        
        # Find the dept_id column and check its foreign key
        dept_id_column = next(col for col in schema.columns if col.name == "dept_id")
        assert dept_id_column.foreign_key is not None
        assert dept_id_column.foreign_key.referenced_table == "departments"
        assert dept_id_column.foreign_key.referenced_column == "id"

    finally:
        cleanup_test_data()


def test_create_table_with_multiple_data_types():
    """Test CREATE TABLE with various data types."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        result = processor.execute_query(
            "CREATE TABLE mixed_types (id INTEGER, name VARCHAR(50), code CHAR(10), score FLOAT)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("mixed_types")
        assert schema is not None
        
        columns_by_name = {col.name: col for col in schema.columns}
        assert columns_by_name["id"].data_type == DataType.INTEGER
        assert columns_by_name["name"].data_type == DataType.VARCHAR
        assert columns_by_name["name"].max_length == 50
        assert columns_by_name["code"].data_type == DataType.CHAR
        assert columns_by_name["code"].max_length == 10
        assert columns_by_name["score"].data_type == DataType.FLOAT

    finally:
        cleanup_test_data()


def test_create_table_duplicate_fails():
    """Test that creating a table with duplicate name fails."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        # Create first table
        processor.execute_query("CREATE TABLE users (id INTEGER)")
        
        # Attempt to create duplicate should fail
        with pytest.raises(ValueError) as excinfo:
            processor.execute_query("CREATE TABLE users (name VARCHAR(50))")
        
        assert "already exists" in str(excinfo.value)

    finally:
        cleanup_test_data()


def test_create_table_duplicate_column_names_fails():
    """Test that duplicate column names in same table fail."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        with pytest.raises(ValueError) as excinfo:
            processor.execute_query("CREATE TABLE users (id INTEGER, id VARCHAR(50))")
        
        assert "duplicate" in str(excinfo.value).lower()

    finally:
        cleanup_test_data()


def test_create_table_invalid_foreign_key_table_fails():
    """Test that foreign key referencing non-existent table fails."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        with pytest.raises(ValueError) as excinfo:
            processor.execute_query(
                "CREATE TABLE employees (id INTEGER, dept_id INTEGER REFERENCES departments(id))"
            )
        
        assert "does not exist" in str(excinfo.value)

    finally:
        cleanup_test_data()


def test_create_table_invalid_foreign_key_column_fails():
    """Test that foreign key referencing non-existent column fails."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        with pytest.raises(ValueError) as excinfo:
            processor.execute_query(
                "CREATE TABLE employees (id INTEGER, dept_id INTEGER REFERENCES departments(nonexistent))"
            )
        
        assert "does not exist" in str(excinfo.value)

    finally:
        cleanup_test_data()


def test_create_table_invalid_data_type_fails():
    """Test that unsupported data types fail."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        with pytest.raises(SyntaxError) as excinfo:
            processor.execute_query("CREATE TABLE users (id BIGINT)")
        
        assert "syntax error" in str(excinfo.value).lower()

    finally:
        cleanup_test_data()


def test_create_table_malformed_syntax_fails():
    """Test that malformed CREATE TABLE syntax fails."""
    cleanup_test_data()
    processor, storage = setup_processor()

    test_cases = [
        "CREATE TABLE users",  # No column definitions
        "CREATE TABLE users ()",  # Empty column definitions
        "CREATE TABLE users id INTEGER",  # Missing parentheses
        "CREATE TABLE users (id)",  # Missing data type
    ]

    try:
        for query in test_cases:
            with pytest.raises((ValueError, SyntaxError)):
                processor.execute_query(query)

    finally:
        cleanup_test_data()


def test_create_table_complex_scenario():
    """Test CREATE TABLE with complex scenario including multiple constraints."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            """CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                employee_code CHAR(8) NOT NULL,
                full_name VARCHAR(200) NOT NULL,
                email VARCHAR(100),
                salary FLOAT,
                department_id INTEGER REFERENCES departments(id)
            )"""
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        assert schema.primary_key == "id"
        assert len(schema.columns) == 6
        
        columns_by_name = {col.name: col for col in schema.columns}
        
        # Verify all columns exist and have correct properties
        assert columns_by_name["id"].primary_key is True
        assert columns_by_name["id"].nullable is False
        
        assert columns_by_name["employee_code"].data_type == DataType.CHAR
        assert columns_by_name["employee_code"].max_length == 8
        assert columns_by_name["employee_code"].nullable is False
        
        assert columns_by_name["full_name"].data_type == DataType.VARCHAR
        assert columns_by_name["full_name"].max_length == 200
        assert columns_by_name["full_name"].nullable is False
        
        assert columns_by_name["email"].nullable is True
        
        assert columns_by_name["department_id"].foreign_key is not None
        assert columns_by_name["department_id"].foreign_key.referenced_table == "departments"

    finally:
        cleanup_test_data()


def test_create_table_with_multiple_constraints_per_column():
    """Test CREATE TABLE with multiple constraints per column (enhanced grammar)."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY NOT NULL, dept_id INTEGER NOT NULL REFERENCES departments(id))"
        )

        assert isinstance(result, ExecutionResult)
        assert "employees" in storage.list_tables()
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        assert schema.primary_key == "id"
        
        columns_by_name = {col.name: col for col in schema.columns}
        
        # Verify id column has both PRIMARY KEY and NOT NULL
        id_column = columns_by_name["id"]
        assert id_column.primary_key is True
        assert id_column.nullable is False
        
        # Verify dept_id column has both NOT NULL and foreign key
        dept_column = columns_by_name["dept_id"]
        assert dept_column.nullable is False
        assert dept_column.foreign_key is not None
        assert dept_column.foreign_key.referenced_table == "departments"

    finally:
        cleanup_test_data()


def test_create_table_with_foreign_key_cascade():
    """Test CREATE TABLE with foreign key CASCADE actions."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE ON UPDATE CASCADE)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        
        dept_column = next(col for col in schema.columns if col.name == "dept_id")
        assert dept_column.foreign_key is not None
        assert dept_column.foreign_key.referenced_table == "departments"
        assert dept_column.foreign_key.referenced_column == "id"
        assert dept_column.foreign_key.on_delete == ForeignKeyAction.CASCADE
        assert dept_column.foreign_key.on_update == ForeignKeyAction.CASCADE

    finally:
        cleanup_test_data()


def test_create_table_with_foreign_key_set_null():
    """Test CREATE TABLE with foreign key SET NULL actions."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE SET NULL ON UPDATE SET NULL)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        
        dept_column = next(col for col in schema.columns if col.name == "dept_id")
        assert dept_column.foreign_key is not None
        assert dept_column.foreign_key.on_delete == ForeignKeyAction.SET_NULL
        assert dept_column.foreign_key.on_update == ForeignKeyAction.SET_NULL

    finally:
        cleanup_test_data()


def test_create_table_with_foreign_key_no_action():
    """Test CREATE TABLE with foreign key NO ACTION actions."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE NO ACTION ON UPDATE NO ACTION)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        
        dept_column = next(col for col in schema.columns if col.name == "dept_id")
        assert dept_column.foreign_key is not None
        assert dept_column.foreign_key.on_delete == ForeignKeyAction.NO_ACTION
        assert dept_column.foreign_key.on_update == ForeignKeyAction.NO_ACTION

    finally:
        cleanup_test_data()


def test_create_table_with_foreign_key_mixed_actions():
    """Test CREATE TABLE with different foreign key actions for DELETE and UPDATE."""
    cleanup_test_data()
    processor, storage = setup_processor_with_existing_table()

    try:
        result = processor.execute_query(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE ON UPDATE RESTRICT)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("employees")
        assert schema is not None
        
        dept_column = next(col for col in schema.columns if col.name == "dept_id")
        assert dept_column.foreign_key is not None
        assert dept_column.foreign_key.on_delete == ForeignKeyAction.CASCADE
        assert dept_column.foreign_key.on_update == ForeignKeyAction.RESTRICT

    finally:
        cleanup_test_data()


def test_create_table_with_explicit_null():
    """Test CREATE TABLE with explicit NULL constraint."""
    cleanup_test_data()
    processor, storage = setup_processor()

    try:
        result = processor.execute_query(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, description VARCHAR(200) NULL)"
        )

        assert isinstance(result, ExecutionResult)
        
        schema = storage.get_table_schema("products")
        assert schema is not None
        
        columns_by_name = {col.name: col for col in schema.columns}
        assert columns_by_name["id"].nullable is False  # PRIMARY KEY
        assert columns_by_name["description"].nullable is True  # Explicitly NULL

    finally:
        cleanup_test_data()