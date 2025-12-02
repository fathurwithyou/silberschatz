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


def cleanup_test_data():
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def setup_employees_table():
    """Setup employee table dengan data untuk testing"""
    storage = StorageManager("data_test")

    schema = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=100),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
            ColumnDefinition(name="department", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="salary", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )

    storage.create_table(schema)

    employees_data = Rows(
        data=[
            {"id": 1, "name": "Alice", "age": 30, "department": "Engineering", "salary": 75000.0},
            {"id": 2, "name": "Bob", "age": 25, "department": "Sales", "salary": 60000.0},
            {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering", "salary": 90000.0},
            {"id": 4, "name": "Diana", "age": 28, "department": "HR", "salary": 70000.0},
            {"id": 5, "name": "Eve", "age": 32, "department": "Engineering", "salary": 85000.0},
            {"id": 6, "name": "Frank", "age": 27, "department": "Sales", "salary": 65000.0},
            {"id": 7, "name": "Grace", "age": 31, "department": "HR", "salary": 72000.0},
            {"id": 8, "name": "Henry", "age": 29, "department": "Engineering", "salary": 78000.0},
            {"id": 9, "name": "Ivy", "age": 26, "department": "Sales", "salary": 62000.0},
            {"id": 10, "name": "Jack", "age": 33, "department": "Engineering", "salary": 88000.0},
        ],
        rows_count=10
    )

    storage.dml_manager.save_all_rows("employees", employees_data, schema)

    return storage, schema, employees_data


#1
def test_create_index_basic():
    print_test("TEST 1: Create Index on Age Column")

    storage, schema, _ = setup_employees_table()

    # Create index on age column
    storage.set_index("employees", "age", "BTREE")

    print("✓ Created index on employees(age)")

    # Verify index exists
    has_index = storage.has_index("employees", "age")
    print(f"Index exists: {has_index}")

    match = has_index

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#2
def test_populate_and_search_index():
    print_test("TEST 2: Populate Index and Search")

    storage, schema, employees_data = setup_employees_table()

    # Create and get index
    storage.set_index("employees", "age", "BTREE")
    index = storage.indexes[("employees", "age")]

    # Populate index with employee ages
    rows = storage.dml_manager.load_all_rows("employees", schema)
    for i, row in enumerate(rows.data):
        age = row.get("age")
        if age is not None:
            index.insert(age, i)

    print(f"✓ Populated index with {rows.rows_count} records")

    # Search for specific ages
    result_30 = index.search(30)
    result_25 = index.search(25)
    result_99 = index.search(99)

    print(f"\nSearch results:")
    print(f"  Age 30: row_ids={result_30}")
    if result_30:
        for row_id in result_30:
            print(f"    -> {rows.data[row_id]['name']}, age={rows.data[row_id]['age']}")

    print(f"  Age 25: row_ids={result_25}")
    if result_25:
        for row_id in result_25:
            print(f"    -> {rows.data[row_id]['name']}, age={rows.data[row_id]['age']}")

    print(f"  Age 99: row_ids={result_99} (should be empty)")

    match = (
        len(result_30) > 0 and
        len(result_25) > 0 and
        len(result_99) == 0
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#3
def test_range_search():
    print_test("TEST 3: Range Search on Age")

    storage, schema, _ = setup_employees_table()

    # Create and populate index
    storage.set_index("employees", "age", "BTREE")
    index = storage.indexes[("employees", "age")]

    rows = storage.dml_manager.load_all_rows("employees", schema)
    for i, row in enumerate(rows.data):
        age = row.get("age")
        if age is not None:
            index.insert(age, i)

    print("✓ Index populated")

    # Range search: age 27-31
    result_range = index.range_search(27, 31)

    print(f"\nRange search age 27-31:")
    print(f"  Found {len(result_range)} employees:")

    for row_id in sorted(result_range):
        employee = rows.data[row_id]
        print(f"    - {employee['name']}, age={employee['age']}")

    # Verify all ages in range
    all_in_range = all(27 <= rows.data[i]['age'] <= 31 for i in result_range)

    match = len(result_range) > 0 and all_in_range

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#4
def test_index_on_department():
    print_test("TEST 4: Index on Department (String Column)")

    storage, schema, _ = setup_employees_table()

    # Create index on department
    storage.set_index("employees", "department", "BTREE")
    index = storage.indexes[("employees", "department")]

    rows = storage.dml_manager.load_all_rows("employees", schema)
    for i, row in enumerate(rows.data):
        dept = row.get("department")
        if dept is not None:
            index.insert(dept, i)

    print("✓ Index on department column created and populated")

    # Search for specific departments
    result_eng = index.search("Engineering")
    result_sales = index.search("Sales")
    result_hr = index.search("HR")

    print(f"\nSearch results:")
    print(f"  Engineering: {len(result_eng)} employees")
    for row_id in result_eng:
        print(f"    - {rows.data[row_id]['name']}")

    print(f"  Sales: {len(result_sales)} employees")
    for row_id in result_sales:
        print(f"    - {rows.data[row_id]['name']}")

    print(f"  HR: {len(result_hr)} employees")
    for row_id in result_hr:
        print(f"    - {rows.data[row_id]['name']}")

    match = (
        len(result_eng) > 0 and
        len(result_sales) > 0 and
        len(result_hr) > 0
    )

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#5
def test_multiple_indexes():
    print_test("TEST 5: Multiple Indexes on Same Table")

    storage, schema, _ = setup_employees_table()

    # Create indexes on multiple columns
    storage.set_index("employees", "age", "BTREE")
    storage.set_index("employees", "department", "BTREE")

    print("✓ Created indexes on age and department")

    # Verify both exist
    has_age = storage.has_index("employees", "age")
    has_dept = storage.has_index("employees", "department")
    has_salary = storage.has_index("employees", "salary")

    print(f"\nIndex status:")
    print(f"  employees(age): {has_age}")
    print(f"  employees(department): {has_dept}")
    print(f"  employees(salary): {has_salary}")

    match = has_age and has_dept and not has_salary

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#6
def test_drop_index():
    print_test("TEST 6: Drop Index")

    storage, schema, _ = setup_employees_table()

    # Create index
    storage.set_index("employees", "age", "BTREE")
    print("✓ Created index on employees(age)")

    has_before = storage.has_index("employees", "age")
    print(f"Index exists before drop: {has_before}")

    # Drop index
    storage.drop_index("employees", "age")
    print("✓ Dropped index")

    has_after = storage.has_index("employees", "age")
    print(f"Index exists after drop: {has_after}")

    match = has_before and not has_after

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#7
def test_duplicate_department_values():
    print_test("TEST 7: Index with Duplicate Values (Department)")

    storage, schema, _ = setup_employees_table()

    # Create index on department (has duplicates)
    storage.set_index("employees", "department", "BTREE")
    index = storage.indexes[("employees", "department")]

    rows = storage.dml_manager.load_all_rows("employees", schema)
    for i, row in enumerate(rows.data):
        dept = row.get("department")
        if dept is not None:
            index.insert(dept, i)

    print("✓ Index populated with duplicate department values")

    # Search Engineering (should have multiple employees)
    result = index.search("Engineering")

    print(f"\nEngineering employees (count: {len(result)}):")
    for row_id in result:
        print(f"  - {rows.data[row_id]['name']}")

    # Verify we got multiple results
    match = len(result) > 1

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#8
def test_index_persistence():
    print_test("TEST 8: Index Persistence (Save/Load)")

    storage1, schema, _ = setup_employees_table()

    # Create and populate index
    storage1.set_index("employees", "age", "BTREE")
    index1 = storage1.indexes[("employees", "age")]

    rows = storage1.dml_manager.load_all_rows("employees", schema)
    for i, row in enumerate(rows.data):
        age = row.get("age")
        if age is not None:
            index1.insert(age, i)

    print("✓ Storage1: Index created and populated")

    result1 = index1.search(30)
    print(f"Storage1: Search age=30 -> {result1}")

    # Create new storage (simulates restart)
    storage2 = StorageManager("data_test")
    storage2.set_index("employees", "age", "BTREE")
    index2 = storage2.indexes[("employees", "age")]

    # Should load from disk
    result2 = index2.search(30)
    print(f"Storage2: Search age=30 -> {result2}")

    match = result1 == result2 and len(result2) > 0

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#9
def test_large_range_search():
    print_test("TEST 9: Large Range Search")

    storage, schema, _ = setup_employees_table()

    # Create and populate index
    storage.set_index("employees", "age", "BTREE")
    index = storage.indexes[("employees", "age")]

    rows = storage.dml_manager.load_all_rows("employees", schema)
    for i, row in enumerate(rows.data):
        age = row.get("age")
        if age is not None:
            index.insert(age, i)

    print("✓ Index populated")

    # Large range that includes all employees
    result_all = index.range_search(20, 40)

    print(f"\nRange 20-40 (all employees):")
    print(f"  Found {len(result_all)} employees")

    ages = sorted([rows.data[i]['age'] for i in result_all])
    print(f"  Ages: {ages}")

    match = len(result_all) == 10

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


#10
def test_error_duplicate_index():
    print_test("TEST 10: Error - Duplicate Index Creation")

    storage, schema, _ = setup_employees_table()

    # Create first index
    storage.set_index("employees", "age", "BTREE")
    print("✓ First index created")

    # Try to create duplicate
    try:
        storage.set_index("employees", "age", "BTREE")
        print("✗ ERROR: Should have raised ValueError")
        match = False
    except ValueError as e:
        print(f"✓ Caught expected error: {e}")
        match = True

    print(f"\nResult: {'PASS' if match else 'FAIL'}")
    return match


def main():
    print_section("B+ TREE INDEX TEST SUITE")

    tests = [
        ("Create Index Basic", test_create_index_basic),
        ("Populate and Search Index", test_populate_and_search_index),
        ("Range Search", test_range_search),
        ("Index on String Column", test_index_on_department),
        ("Multiple Indexes", test_multiple_indexes),
        ("Drop Index", test_drop_index),
        ("Duplicate Values", test_duplicate_department_values),
        ("Index Persistence", test_index_persistence),
        ("Large Range Search", test_large_range_search),
        ("Error Handling - Duplicate Index", test_error_duplicate_index),
    ]

    results = []

    for test_name, test_func in tests:
        cleanup_test_data()
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
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:8} {test_name}")

    print(f"\n{'='*70}")
    print(f"Total: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    print(f"{'='*70}")

    cleanup_test_data()

    return 0 if passed == total else 1


if __name__ == "__main__":
    exit(main())
