import pytest
import os
import shutil
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.storage.storage_manager import StorageManager
from src.core.models import (
    TableSchema,
    ColumnDefinition,
    DataType,
    ForeignKeyConstraint,
    ForeignKeyAction
)


@pytest.fixture(scope="function")
def test_data_dir():
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    yield test_dir
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


@pytest.fixture(scope="function")
def storage(test_data_dir):
    return StorageManager("data_test")


class TestCreateTable:
    
    def test_create_table_basic(self, storage):
        schema = TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="salary", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        storage.create_table(schema)
        
        retrieved = storage.get_table_schema("employees")
        
        assert schema.table_name == retrieved.table_name
        assert len(schema.columns) == len(retrieved.columns)
    
    def test_create_table_duplicate(self, storage):
        schema = TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ],
            primary_key="id"
        )
        
        storage.create_table(schema)
        
        with pytest.raises(ValueError, match="already exists"):
            storage.create_table(schema)
    
    def test_create_table_with_foreign_key(self, storage):
        users_schema = TableSchema(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True)
            ],
            primary_key="id"
        )
        
        storage.create_table(users_schema)
        
        orders_schema = TableSchema(
            table_name="customer_orders",
            columns=[
                ColumnDefinition(name="order_id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="user_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint(
                        referenced_table="users",
                        referenced_column="id",
                        on_delete=ForeignKeyAction.CASCADE,
                        on_update=ForeignKeyAction.CASCADE
                    )
                ),
            ],
            primary_key="order_id"
        )
        
        storage.create_table(orders_schema)
        
        retrieved = storage.get_table_schema("customer_orders")
        fk = retrieved.columns[1].foreign_key
        
        assert fk is not None
        assert fk.referenced_table == "users"
        assert fk.referenced_column == "id"
        assert fk.on_delete == ForeignKeyAction.CASCADE


class TestDropTable:
    
    def test_drop_table(self, storage):
        schema = TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ],
            primary_key="id"
        )
        
        storage.create_table(schema)
        
        tables_before = storage.list_tables()
        assert "employees" in tables_before
        
        storage.drop_table("employees")
        
        tables_after = storage.list_tables()
        assert "employees" not in tables_after
    
    def test_drop_nonexistent_table(self, storage):
        with pytest.raises(ValueError, match="does not exist"):
            storage.drop_table("nonexistent")


class TestListTables:
    
    def test_list_tables(self, storage):
        schemas = [
            TableSchema(
                table_name="users",
                columns=[ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True)],
                primary_key="id"
            ),
            TableSchema(
                table_name="products",
                columns=[ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True)],
                primary_key="id"
            ),
            TableSchema(
                table_name="orders",
                columns=[ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True)],
                primary_key="id"
            ),
        ]
        
        for schema in schemas:
            storage.create_table(schema)
        
        tables = storage.list_tables()
        
        assert len(tables) == 3
        assert all(s.table_name in tables for s in schemas)
        assert set(tables) == {"users", "products", "orders"}
    
    def test_list_tables_empty(self, storage):
        tables = storage.list_tables()
        
        assert len(tables) == 0


class TestGetSchema:
    
    def test_get_existing_schema(self, storage):
        schema = TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ],
            primary_key="id"
        )
        
        storage.create_table(schema)
        
        retrieved = storage.get_table_schema("employees")
        
        assert retrieved is not None
        assert retrieved.table_name == "employees"
        assert len(retrieved.columns) == 2
    
    def test_get_nonexistent_schema(self, storage):
        schema = storage.get_table_schema("nonexistent_table")
        
        assert schema is None


class TestSchemaValidation:
    
    def test_empty_table_name(self, storage):
        schema = TableSchema(
            table_name="",
            columns=[],
            primary_key=None
        )
        
        with pytest.raises(ValueError, match="Table name cannot be empty"):
            storage.create_table(schema)
    
    def test_no_columns(self, storage):
        schema = TableSchema(
            table_name="test",
            columns=[],
            primary_key=None
        )
        
        with pytest.raises(ValueError, match="must have at least one column"):
            storage.create_table(schema)
    
    def test_duplicate_columns(self, storage):
        schema = TableSchema(
            table_name="test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER),
                ColumnDefinition(name="id", data_type=DataType.INTEGER),
            ],
            primary_key=None
        )
        
        with pytest.raises(ValueError, match="Duplicate column name"):
            storage.create_table(schema)
    
    def test_invalid_primary_key(self, storage):
        schema = TableSchema(
            table_name="test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER),
            ],
            primary_key="nonexistent"
        )
        
        with pytest.raises(ValueError, match="Primary key.*not found"):
            storage.create_table(schema)
    
    def test_foreign_key_referenced_table_not_exists(self, storage):
        schema = TableSchema(
            table_name="orders",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="user_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint(
                        referenced_table="users",
                        referenced_column="id",
                        on_delete=ForeignKeyAction.CASCADE,
                        on_update=ForeignKeyAction.CASCADE
                    )
                ),
            ],
            primary_key="id"
        )
        
        with pytest.raises(ValueError, match="Referenced table.*does not exist"):
            storage.create_table(schema)
    
    def test_foreign_key_referenced_column_not_exists(self, storage):
        users_schema = TableSchema(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True)
            ],
            primary_key="id"
        )
        
        storage.create_table(users_schema)
        
        orders_schema = TableSchema(
            table_name="orders",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="user_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint(
                        referenced_table="users",
                        referenced_column="nonexistent",
                        on_delete=ForeignKeyAction.CASCADE,
                        on_update=ForeignKeyAction.CASCADE
                    )
                ),
            ],
            primary_key="id"
        )
        
        with pytest.raises(ValueError, match="Referenced column.*not found"):
            storage.create_table(orders_schema)