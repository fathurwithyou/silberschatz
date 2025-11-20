import sys
import os
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.storage.storage_manager import StorageManager
from src.core.models import (
    TableSchema,
    ColumnDefinition,
    DataType,
    Rows
)


def print_section(title: str):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_test(test_name: str):
    print(f"\n[{test_name}]")
    print("-" * 70)


def print_table_data(rows: Rows):
    if rows.rows_count == 0:
        print("(Empty table)")
    else:
        for row in rows.data:
            print(row)
    print(f"Total rows: {rows.rows_count}\n")
    print("\n")


def cleanup_test_data():
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def setup_test_table_with_data():
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

    return storage, test_rows


#1
def test_get_stats_basic():
    print_test("TEST 1: Get Stat -> Basic stats")

    storage, test_rows = setup_test_table_with_data()
    stats = storage.get_stats("employees")
    print(f"Statistics:")
    print(f"Table name: {stats.table_name}")
    print_table_data(test_rows)


    print(f"n_r (number of rows): {stats.n_r}")
    print(f"l_r (tuple size): {stats.l_r} bytes")
    print(f"f_r (blocking factor): {stats.f_r} tuples/block")
    print(f"b_r (number of blocks): {stats.b_r} blocks")
    print(f"V (distinct values): {stats.V}")

    match = (
        stats.table_name == "employees" and
        stats.n_r == 5 and
        stats.l_r > 0 and
        stats.f_r > 0 and
        stats.b_r > 0 and
        stats.V["id"] == 5 and  
        stats.V["name"] == 5 and  
        stats.V["age"] == 5 and  
        stats.V["salary"] == 5  
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#2
def test_get_stats_min_max_values():
    print_test("TEST 2: Get Stat -> Min/Max Values")

    storage = StorageManager("data_test")
    
    schema = storage.get_table_schema("employees")
    rows = storage.dml_manager.load_all_rows("employees", schema)
    stats = storage.get_stats("employees")

    print(f"Statistics:")
    print(f"Table name: {stats.table_name}")
    print_table_data(rows)


    print(f"Min values: {stats.min_values}")
    print(f"Max values: {stats.max_values}")

    match = (
        stats.min_values is not None and
        stats.max_values is not None and
        stats.min_values.get("age") == 25 and
        stats.max_values.get("age") == 35 and
        stats.min_values.get("salary") == 60000.0 and
        stats.max_values.get("salary") == 90000.0 and
        "name" not in stats.min_values and  
        "name" not in stats.max_values
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#3
def test_get_stats_no_nulls():
    print_test("TEST 3: Get Stat -> Null Counts is None (no Nulls)")

    storage = StorageManager("data_test")
    
    schema = storage.get_table_schema("employees")
    rows = storage.dml_manager.load_all_rows("employees", schema)
    stats = storage.get_stats("employees")

    print(f"Statistics:")
    print(f"Table name: {stats.table_name}")
    print_table_data(rows)

    
    print(f"Null counts: {stats.null_counts}")

    match = stats.null_counts is None

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#4
def test_get_stats_with_nulls():
    print_test("TEST 4: Get Stat -> NULL values (null counts) exist")

    storage = StorageManager("data_test")

    schema = TableSchema(
        table_name="products",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50, nullable=True),
            ColumnDefinition(name="price", data_type=DataType.FLOAT, nullable=True),
        ],
        primary_key="id"
    )

    storage.create_table(schema)

    test_rows = Rows(
        data=[
            {"id": 1, "name": "Laptop", "price": 1000.0},
            {"id": 2, "name": None, "price": 200.0},
            {"id": 3, "name": "Mouse", "price": None},
            {"id": 4, "name": None, "price": None},
        ],
        rows_count=4
    )

    storage.dml_manager.save_all_rows("products", test_rows, schema)
    stats = storage.get_stats("products")
    
    print(f"Statistics:")
    print(f"Table name: {stats.table_name}")
    print_table_data(test_rows)
    
    print(f"Null counts: {stats.null_counts}")

    match = (
        stats.null_counts is not None and
        stats.null_counts.get("name") == 2 and
        stats.null_counts.get("price") == 2 and
        "id" not in stats.null_counts  
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#5
def test_get_stats_no_numeric_columns():
    print_test("TEST 5: Get Stat -> No Numeric Columns")

    storage = StorageManager("data_test")

    text_schema = TableSchema(
        table_name="product_desc",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.VARCHAR, max_length=10, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="description", data_type=DataType.CHAR, max_length=100),
        ],
        primary_key="id"
    )

    storage.create_table(text_schema)

    test_rows = Rows(
        data=[
            {"id": "A1", "name": "Laptop", "description": "Mobile computer"},
            {"id": "A2", "name": "Mouse", "description": "Looks like a mouse"},
        ],
        rows_count=2
    )

    storage.dml_manager.save_all_rows("product_desc", test_rows, text_schema)
    stats = storage.get_stats("product_desc")
    
    print(f"Statistics:")
    print(f"Table name: {stats.table_name}")
    print_table_data(test_rows)


    print(f"min_values: {stats.min_values}")
    print(f"max_values: {stats.max_values}")

    match = (
        stats.n_r == 2 and
        stats.min_values is None and
        stats.max_values is None
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#6
def test_get_stats_empty_table():
    print_test("TEST 6: Get Stat -> Empty Table")

    storage = StorageManager("data_test")

    empty_schema = TableSchema(
        table_name="empty_table",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="value", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )

    storage.create_table(empty_schema)
    
    rows = storage.dml_manager.load_all_rows("empty_table", empty_schema)
    stats = storage.get_stats("empty_table")
    
    print(f"Statistics:")
    print(f"Table name: {stats.table_name}")
    print_table_data(rows)


    print(f"n_r: {stats.n_r}")
    print(f"b_r: {stats.b_r}")
    print(f"V: {stats.V}")
    print(f"min_values: {stats.min_values}")
    print(f"max_values: {stats.max_values}")
    print(f"null_counts: {stats.null_counts}")

    match = (
        stats.n_r == 0 and
        stats.b_r == 0 and
        all(v == 0 for v in stats.V.values()) and
        stats.min_values is None and
        stats.max_values is None and
        stats.null_counts is None
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#7
def test_get_stats_nonexistent_table():
    print_test("TEST 7: Get Stat -> Nonexistent Table")

    storage = StorageManager("data_test")

    try:
        storage.get_stats("nonexistent")
        print("Result: FAIL (Should have raised error)")
        return False
    except ValueError as e:
        print(f"Error caught: {e}")
        print("Result: PASS")
        return True


def main():
    print_section("STATISTICS (Get Stat) TEST")
    cleanup_test_data()

    tests = [
        ("Get Stat Basic", test_get_stats_basic),
        ("Get Stat Min/Max Values", test_get_stats_min_max_values),
        ("Get Stat With No Null Values (Null counts none)", test_get_stats_no_nulls),
        ("Get Stat With NULLs (Null Counts exist)", test_get_stats_with_nulls),
        ("Get Stat No Numeric Columns", test_get_stats_no_numeric_columns),
        ("Get Stat Empty Table", test_get_stats_empty_table),
        ("Get Stat Nonexistent Table", test_get_stats_nonexistent_table),
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