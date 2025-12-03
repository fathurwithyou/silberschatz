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
    DataRetrieval,
    DataWrite,
    DataDeletion,
    Condition,
    ComparisonOperator,
    Rows
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


@pytest.fixture(scope="function")
def employees_table(storage):
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
    
    storage.create_table(schema)
    
    test_rows = Rows(
        data=[
            {"id": 1, "name": "Alice", "age": 30, "salary": 75000.0},
            {"id": 2, "name": "Bob", "age": 25, "salary": 60000.0},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 90000.0},
            {"id": 4, "name": "Diana", "age": 28, "salary": 70000.0},
            {"id": 5, "name": "Eve", "age": 32, "salary": 85000.0},
        ],
        rows_count=5
    )
    
    storage.dml_manager.save_all_rows("employees", test_rows, schema)
    
    return storage


class TestReadOperations:
    
    def test_read_all_rows(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"]
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 5
        assert len(result.data) == 5
    
    def test_read_with_projection(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["id", "name"]
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 5
        assert all(set(row.keys()) == {"id", "name"} for row in result.data)
    
    def test_read_with_condition(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[
                Condition(column="age", operator=ComparisonOperator.GT, value=30)
            ]
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 2
        assert all(row["age"] > 30 for row in result.data)
    
    def test_read_with_multiple_conditions(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[
                Condition(column="age", operator=ComparisonOperator.GE, value=28),
                Condition(column="salary", operator=ComparisonOperator.LT, value=80000.0)
            ]
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 2
        assert all(row["age"] >= 28 and row["salary"] < 80000.0 for row in result.data)
    
    def test_read_with_limit(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            limit=3
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 3
    
    def test_read_with_offset(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            offset=2
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 3
    
    def test_read_with_limit_offset(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            limit=2,
            offset=1
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 2
    
    def test_read_empty_table(self, storage):
        empty_schema = TableSchema(
            table_name="empty_table",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ],
            primary_key="id"
        )
        
        storage.create_table(empty_schema)
        
        retrieval = DataRetrieval(
            table_name="empty_table",
            columns=["*"]
        )
        
        result = storage.read_block(retrieval)
        
        assert result.rows_count == 0
    
    def test_read_nonexistent_table(self, storage):
        retrieval = DataRetrieval(
            table_name="nonexistent",
            columns=["*"]
        )
        
        with pytest.raises(ValueError, match="does not exist"):
            storage.read_block(retrieval)
    
    def test_read_complex_query(self, employees_table):
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["name", "salary"],
            conditions=[
                Condition(column="salary", operator=ComparisonOperator.GE, value=70000.0)
            ],
            limit=2
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count == 2
        assert all(set(row.keys()) == {"name", "salary"} for row in result.data)
        assert all(row["salary"] >= 70000.0 for row in result.data)


class TestWriteOperations:
    
    def test_insert(self, employees_table):
        dw = DataWrite(
            table_name="employees",
            data={"id": 999, "name": "New Guy", "age": 27, "salary": 72000},
            is_update=False,
            conditions=[]
        )
        
        inserted = employees_table.write_block(dw)
        
        result = employees_table.read_block(DataRetrieval(
            table_name="employees", 
            columns=["*"], 
            conditions=[], 
            limit=None, 
            offset=0
        ))
        
        assert inserted == 1
        assert result.rows_count == 6
    
    def test_update(self, employees_table):
        dw = DataWrite(
            table_name="employees",
            data={"salary": 88000},
            is_update=True,
            conditions=[Condition(column="age", operator=ComparisonOperator.GE, value=30)]
        )
        
        updated = employees_table.write_block(dw)
        
        result = employees_table.read_block(DataRetrieval(
            table_name="employees", 
            columns=["*"], 
            conditions=[], 
            limit=None, 
            offset=0
        ))
        
        assert updated == 3
        assert all(row["salary"] == 88000 for row in result.data if row["age"] >= 30)
    
    def test_delete(self, employees_table):
        dd = DataDeletion(
            table_name="employees",
            conditions=[Condition(column="salary", operator=ComparisonOperator.LT, value=70000)]
        )
        
        deleted = employees_table.delete_block(dd)
        
        result = employees_table.read_block(DataRetrieval(
            table_name="employees", 
            columns=["*"], 
            conditions=[], 
            limit=None, 
            offset=0
        ))
        
        assert deleted == 1
        assert result.rows_count == 4


class TestIndexOperations:
    
    def test_create_index(self, employees_table):
        employees_table.set_index("employees", "age", "b_plus_tree")
        
        assert employees_table.has_index("employees", "age")
    
    def test_read_with_index(self, employees_table):
        employees_table.set_index("employees", "age", "b_plus_tree")
        
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[
                Condition(column="age", operator=ComparisonOperator.EQ, value=30)
            ]
        )
        
        result = employees_table.read_block(retrieval)
        
        assert result.rows_count >= 1
        assert all(row["age"] == 30 for row in result.data)
    
    def test_insert_with_index(self, employees_table):
        employees_table.set_index("employees", "age", "b_plus_tree")
        
        dw = DataWrite(
            table_name="employees",
            data={"id": 1000, "name": "Frank", "age": 30, "salary": 95000},
            is_update=False,
            conditions=[]
        )
        
        inserted = employees_table.write_block(dw)
        
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[
                Condition(column="age", operator=ComparisonOperator.EQ, value=30)
            ]
        )
        
        result = employees_table.read_block(retrieval)
        
        assert inserted == 1
        assert result.rows_count == 2
    
    def test_update_with_index(self, employees_table):
        employees_table.set_index("employees", "age", "b_plus_tree")
        
        dw = DataWrite(
            table_name="employees",
            data={"age": 31},
            is_update=True,
            conditions=[Condition(column="age", operator=ComparisonOperator.EQ, value=30)]
        )
        
        updated = employees_table.write_block(dw)
        
        retrieval_old = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[Condition(column="age", operator=ComparisonOperator.EQ, value=30)]
        )
        result_old = employees_table.read_block(retrieval_old)
        
        retrieval_new = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[Condition(column="age", operator=ComparisonOperator.EQ, value=31)]
        )
        result_new = employees_table.read_block(retrieval_new)
        
        assert updated == 1
        assert result_old.rows_count == 0
        assert result_new.rows_count == 1
    
    def test_delete_with_index(self, employees_table):
        employees_table.set_index("employees", "age", "b_plus_tree")
        
        dd = DataDeletion(
            table_name="employees",
            conditions=[Condition(column="age", operator=ComparisonOperator.EQ, value=30)]
        )
        
        deleted = employees_table.delete_block(dd)
        
        retrieval = DataRetrieval(
            table_name="employees",
            columns=["*"],
            conditions=[Condition(column="age", operator=ComparisonOperator.EQ, value=30)]
        )
        result = employees_table.read_block(retrieval)
        
        assert deleted == 1
        assert result.rows_count == 0
    
    def test_drop_index(self, employees_table):
        employees_table.set_index("employees", "age", "b_plus_tree")
        
        employees_table.drop_index("employees", "age")
        
        assert not employees_table.has_index("employees", "age")


class TestConstraints:
    
    def test_insert_duplicate_pk(self, employees_table):
        dw = DataWrite(
            table_name="employees",
            data={"id": 1, "name": "Duplicate", "age": 99, "salary": 99999},
            is_update=False,
            conditions=[]
        )
        
        with pytest.raises(ValueError, match="Duplicate primary key"):
            employees_table.write_block(dw)