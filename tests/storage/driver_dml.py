import sys
import os
import shutil

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


def print_section(title: str):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_test(test_name: str):
    print(f"\n[{test_name}]")
    print("-" * 70)


def cleanup_test_data():
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def setup_test_table():
    storage = StorageManager("data_test")
    
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


def test_read_all_rows():
    print_test("TEST 1: Read All Rows")
    
    storage = setup_test_table()
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["*"]
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = result.rows_count == 5
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_with_projection():
    print_test("TEST 2: Read with Column Projection")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["id", "name"]
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = (result.rows_count == 5 and 
             all(set(row.keys()) == {"id", "name"} for row in result.data))
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_with_condition():
    print_test("TEST 3: Read with WHERE Condition")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["*"],
        conditions=[
            Condition(column="age", operator=ComparisonOperator.GT, value=30)
        ]
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = (result.rows_count == 2 and 
             all(row["age"] > 30 for row in result.data))
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_with_multiple_conditions():
    print_test("TEST 4: Read with Multiple Conditions")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["*"],
        conditions=[
            Condition(column="age", operator=ComparisonOperator.GE, value=28),
            Condition(column="salary", operator=ComparisonOperator.LT, value=80000.0)
        ]
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = (result.rows_count == 2 and
             all(row["age"] >= 28 and row["salary"] < 80000.0 for row in result.data))
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_with_limit():
    print_test("TEST 5: Read with LIMIT")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["*"],
        limit=3
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = result.rows_count == 3
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_with_offset():
    print_test("TEST 6: Read with OFFSET")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["*"],
        offset=2
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = result.rows_count == 3
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_with_limit_offset():
    print_test("TEST 7: Read with LIMIT and OFFSET")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["*"],
        limit=2,
        offset=1
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = result.rows_count == 2
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_empty_table():
    print_test("TEST 8: Read Empty Table")
    
    storage = StorageManager("data_test")
    
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
    
    print(f"Rows retrieved: {result.rows_count}")
    
    match = result.rows_count == 0
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_read_nonexistent_table():
    print_test("TEST 9: Read Nonexistent Table")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="nonexistent",
        columns=["*"]
    )
    
    try:
        storage.read_block(retrieval)
        print("Result: FAIL (Should have raised error)")
        return False
    except ValueError as e:
        print(f"Error caught: {e}")
        print("Result: PASS")
        return True


def test_read_complex_query():
    print_test("TEST 10: Complex Query (Projection + Condition + Limit)")
    
    storage = StorageManager("data_test")
    
    retrieval = DataRetrieval(
        table_name="employees",
        columns=["name", "salary"],
        conditions=[
            Condition(column="salary", operator=ComparisonOperator.GE, value=70000.0)
        ],
        limit=2
    )
    
    result = storage.read_block(retrieval)
    
    print(f"Rows retrieved: {result.rows_count}")
    for row in result.data:
        print(f"  {row}")
    
    match = (result.rows_count == 2 and
             all(set(row.keys()) == {"name", "salary"} for row in result.data) and
             all(row["salary"] >= 70000.0 for row in result.data))
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match

def test_insert():
    print_test("TEST 11: Insert")
    
    storage = StorageManager("data_test")

    dw = DataWrite(
        table="employees",
        data={"id": 999, "name": "New Guy", "age": 27, "salary": 72000},
        is_update=False,
        conditions=[]
    )
    inserted = storage.write_block(dw)
    print_test("insert returns 1", inserted == 1)

def test_update():
    print_test("TEST 11: Insert")
    
    storage = StorageManager("data_test")

    dw = DataWrite(
        table="employees",
        data={"salary": 88000},
        is_update=True,
        conditions=[Condition(column="age", operator=ComparisonOperator.GE, value=30)]
    )
    updated = storage.write_block(dw)
    print_test("updated count > 0", updated > 0)

def test_delete():
    print_test("TEST 12: Delete")
    
    storage = StorageManager("data_test")
    dd = DataDeletion(
        table="employees",
        conditions=[Condition(column="salary", operator=ComparisonOperator.LT, value=70000)]
    )
    deleted = storage.delete_block(dd)
    print_test("deleted >= 0", deleted >= 0)

def main():
    print_section("DML READ OPERATIONS TEST")
    cleanup_test_data()
    
    tests = [
        ("Read All Rows", test_read_all_rows),
        ("Read with Projection", test_read_with_projection),
        ("Read with Condition", test_read_with_condition),
        ("Read with Multiple Conditions", test_read_with_multiple_conditions),
        ("Read with LIMIT", test_read_with_limit),
        ("Read with OFFSET", test_read_with_offset),
        ("Read with LIMIT and OFFSET", test_read_with_limit_offset),
        ("Read Empty Table", test_read_empty_table),
        ("Read Nonexistent Table", test_read_nonexistent_table),
        ("Complex Query", test_read_complex_query),
        ("Insert", test_insert),
        ("Update", test_update),
        ("Delete", test_delete),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    print_section("TEST SUMMARY")
    passed = sum(1 for _, res in results if res)
    total = len(results)
    for test_name, result in results:
        print(f"{'PASS' if result else 'FAIL'} {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    cleanup_test_data()


if __name__ == "__main__":
    exit(main())