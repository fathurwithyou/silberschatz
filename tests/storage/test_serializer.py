import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.storage.serializer import Serializer
from src.core.models import (
    TableSchema, 
    ColumnDefinition, 
    DataType, 
    Rows,
    ForeignKeyConstraint,
    ForeignKeyAction
)


@pytest.fixture
def serializer():
    return Serializer()


class TestSingleRowSerialization:
    
    def test_single_row_basic(self, serializer):
        schema = TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="age", data_type=DataType.INTEGER),
                ColumnDefinition(name="salary", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        row = {
            "id": 1,
            "name": "Alice Johnson",
            "age": 25,
            "salary": 75000.50
        }
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized
        assert len(serialized) > 0
    
    def test_single_row_with_null(self, serializer):
        schema = TableSchema(
            table_name="products",
            columns=[
                ColumnDefinition(name="product_id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=100),
                ColumnDefinition(name="description", data_type=DataType.VARCHAR, max_length=255, nullable=True),
                ColumnDefinition(name="price", data_type=DataType.FLOAT),
                ColumnDefinition(name="discount", data_type=DataType.FLOAT, nullable=True),
            ],
            primary_key="product_id"
        )
        
        row = {
            "product_id": 101,
            "name": "Laptop",
            "description": None,
            "price": 15000.00,
            "discount": None
        }
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized
    
    def test_char_vs_varchar(self, serializer):
        schema = TableSchema(
            table_name="test_strings",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="code", data_type=DataType.CHAR, max_length=5),
                ColumnDefinition(name="description", data_type=DataType.VARCHAR, max_length=50),
            ],
            primary_key="id"
        )
        
        row = {
            "id": 1,
            "code": "ABC",
            "description": "Short text"
        }
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized
    
    def test_all_data_types(self, serializer):
        schema = TableSchema(
            table_name="comprehensive",
            columns=[
                ColumnDefinition(name="int_col", data_type=DataType.INTEGER),
                ColumnDefinition(name="float_col", data_type=DataType.FLOAT),
                ColumnDefinition(name="char_col", data_type=DataType.CHAR, max_length=15),
                ColumnDefinition(name="varchar_col", data_type=DataType.VARCHAR, max_length=30),
            ],
            primary_key="int_col"
        )
        
        row = {
            "int_col": 42,
            "float_col": 3.14159,
            "char_col": "Fixed",
            "varchar_col": "Variable length string"
        }
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized


class TestRowsObjectSerialization:
    
    def test_rows_object(self, serializer):
        schema = TableSchema(
            table_name="students",
            columns=[
                ColumnDefinition(name="student_id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="gpa", data_type=DataType.FLOAT),
            ],
            primary_key="student_id"
        )
        
        rows = Rows(
            data=[
                {"student_id": 1, "name": "Alice", "gpa": 3.8},
                {"student_id": 2, "name": "Bob", "gpa": 3.5},
                {"student_id": 3, "name": "Charlie", "gpa": 3.9},
                {"student_id": 4, "name": "Diana", "gpa": 3.7},
            ],
            rows_count=4
        )
        
        serialized = serializer.serialize_rows(rows, schema)
        deserialized = serializer.deserialize_rows(serialized, schema)
        
        assert rows.rows_count == deserialized.rows_count
        assert rows.data == deserialized.data
    
    def test_empty_rows(self, serializer):
        schema = TableSchema(
            table_name="empty_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="value", data_type=DataType.VARCHAR, max_length=10),
            ],
            primary_key="id"
        )
        
        empty_rows = Rows(data=[], rows_count=0)
        
        serialized = serializer.serialize_rows(empty_rows, schema)
        deserialized = serializer.deserialize_rows(serialized, schema)
        
        assert empty_rows.rows_count == deserialized.rows_count
        assert empty_rows.data == deserialized.data


class TestEdgeCases:
    
    def test_empty_string(self, serializer):
        schema = TableSchema(
            table_name="edge_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="text", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="value", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        row = {"id": 1, "text": "", "value": 0.0}
        expected = {"id": 1, "text": None, "value": 0.0} 
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert expected == deserialized  
    
    def test_string_truncation(self, serializer):
        schema = TableSchema(
            table_name="edge_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="text", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="value", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        row = {"id": 2, "text": "A" * 100, "value": 1.5}
        expected = {"id": 2, "text": "A" * 20, "value": 1.5}
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert expected == deserialized
    
    def test_negative_numbers(self, serializer):
        schema = TableSchema(
            table_name="edge_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="text", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="value", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        row = {"id": -999, "text": "Negative", "value": -123.45}
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized
    
    def test_zero_values(self, serializer):
        schema = TableSchema(
            table_name="edge_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="text", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="value", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        row = {"id": 0, "text": "Zero", "value": 0.0}
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized
    
    def test_special_characters(self, serializer):
        schema = TableSchema(
            table_name="edge_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="text", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="value", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        row = {"id": 5, "text": "Hello@#$%^&*()", "value": 99.99}
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        assert row == deserialized


class TestRowSizeCalculation:
    
    def test_row_size_calculation(self, serializer):
        schema = TableSchema(
            table_name="size_test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="dept", data_type=DataType.CHAR, max_length=10),
                ColumnDefinition(name="salary", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        max_size = serializer.calculate_row_size(schema)
        
        row = {"id": 1, "name": "Alice", "dept": "IT", "salary": 75000.0}
        serialized = serializer.serialize_row(row, schema)
        
        assert max_size > 0
        assert len(serialized) > 0
        
        block_size = 4096
        f_r = block_size // max_size
        
        assert f_r > 0


class TestSchemaSerialization:
    
    def test_schema_basic(self, serializer):
        schema = TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="salary", data_type=DataType.FLOAT),
            ],
            primary_key="id"
        )
        
        serialized = serializer.serialize_schema(schema)
        deserialized = serializer.deserialize_schema(serialized)
        
        assert schema.table_name == deserialized.table_name
        assert len(schema.columns) == len(deserialized.columns)
        assert schema.primary_key == deserialized.primary_key
    
    def test_schema_with_foreign_key(self, serializer):
        schema = TableSchema(
            table_name="orders",
            columns=[
                ColumnDefinition(name="order_id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="customer_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint(
                        referenced_table="customers",
                        referenced_column="id",
                        on_delete=ForeignKeyAction.CASCADE,
                        on_update=ForeignKeyAction.CASCADE
                    )
                ),
                ColumnDefinition(name="amount", data_type=DataType.FLOAT),
            ],
            primary_key="order_id"
        )
        
        serialized = serializer.serialize_schema(schema)
        deserialized = serializer.deserialize_schema(serialized)
        
        fk = schema.columns[1].foreign_key
        des_fk = deserialized.columns[1].foreign_key
        
        assert schema.table_name == deserialized.table_name
        assert fk.referenced_table == des_fk.referenced_table
        assert fk.referenced_column == des_fk.referenced_column
        assert fk.on_delete == des_fk.on_delete
        assert fk.on_update == des_fk.on_update
    
    def test_schema_multiple_foreign_keys(self, serializer):
        schema = TableSchema(
            table_name="order_items",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="order_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint(
                        referenced_table="orders",
                        referenced_column="order_id",
                        on_delete=ForeignKeyAction.CASCADE,
                        on_update=ForeignKeyAction.RESTRICT
                    )
                ),
                ColumnDefinition(
                    name="product_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint(
                        referenced_table="products",
                        referenced_column="product_id",
                        on_delete=ForeignKeyAction.RESTRICT,
                        on_update=ForeignKeyAction.CASCADE
                    )
                ),
            ],
            primary_key="id"
        )
        
        fk_count_orig = sum(1 for col in schema.columns if col.foreign_key is not None)
        
        serialized = serializer.serialize_schema(schema)
        deserialized = serializer.deserialize_schema(serialized)
        
        fk_count_des = sum(1 for col in deserialized.columns if col.foreign_key is not None)
        
        assert fk_count_orig == fk_count_des == 2