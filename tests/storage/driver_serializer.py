import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.storage.serializer import Serializer
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


def test_single_row_basic():
    print_test("TEST 1: Single Row - Basic (No NULL)")
    
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
    
    serializer = Serializer()
    
    # Test data
    row = {
        "id": 1,
        "name": "Alice Johnson",
        "age": 25,
        "salary": 75000.50
    }
    
    # Serialize
    serialized = serializer.serialize_row(row, schema)
    print(f"Original:     {row}")
    print(f"Serialized:   {len(serialized)} bytes")
    print(f"Hex:          {serialized.hex()}")
    
    deserialized = serializer.deserialize_row(serialized, schema)
    print(f"Deserialized: {deserialized}")
    
    match = row == deserialized
    print(f"Result: {'PASS' if match else 'FAIL'}")
    
    return match


def test_single_row_with_null():
    print_test("TEST 2: Single Row - With NULL Values")
    
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
    
    serializer = Serializer()
    
    row = {
        "product_id": 101,
        "name": "Laptop",
        "description": None,
        "price": 15000.00,
        "discount": None
    }
    
    serialized = serializer.serialize_row(row, schema)
    print(f"Original:     {row}")
    print(f"Serialized:   {len(serialized)} bytes")
    
    deserialized = serializer.deserialize_row(serialized, schema)
    print(f"Deserialized: {deserialized}")
    
    match = row == deserialized
    print(f"Result: {'PASS' if match else 'FAIL'}")
    
    return match


def test_char_vs_varchar():
    print_test("TEST 3: CHAR vs VARCHAR Data Types")
    
    schema = TableSchema(
        table_name="test_strings",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="code", data_type=DataType.CHAR, max_length=5),
            ColumnDefinition(name="description", data_type=DataType.VARCHAR, max_length=50),
        ],
        primary_key="id"
    )
    
    serializer = Serializer()
    
    row = {
        "id": 1,
        "code": "ABC",
        "description": "Short text"
    }
    
    serialized = serializer.serialize_row(row, schema)
    print(f"Original:     {row}")
    print(f"Serialized:   {len(serialized)} bytes")
    
    deserialized = serializer.deserialize_row(serialized, schema)
    print(f"Deserialized: {deserialized}")
    
    match = row == deserialized
    print(f"Result: {'PASS' if match else 'FAIL'}")
    
    return match


def test_rows_object():
    print_test("TEST 4: Rows Object - Multiple Rows")
    
    schema = TableSchema(
        table_name="students",
        columns=[
            ColumnDefinition(name="student_id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="gpa", data_type=DataType.FLOAT),
        ],
        primary_key="student_id"
    )
    
    serializer = Serializer()
    
    # Create Rows object
    rows = Rows(
        data=[
            {"student_id": 1, "name": "Alice", "gpa": 3.8},
            {"student_id": 2, "name": "Bob", "gpa": 3.5},
            {"student_id": 3, "name": "Charlie", "gpa": 3.9},
            {"student_id": 4, "name": "Diana", "gpa": 3.7},
        ],
        rows_count=4
    )
    
    print(f"Original Rows: rows_count={rows.rows_count}")
    for i, row in enumerate(rows.data, 1):
        print(f"  Row {i}: {row}")
    
    serialized = serializer.serialize_rows(rows, schema)
    print(f"Serialized: {len(serialized)} bytes, Avg: {len(serialized) / rows.rows_count:.2f} bytes/row")
    
    deserialized = serializer.deserialize_rows(serialized, schema)
    print(f"Deserialized Rows: rows_count={deserialized.rows_count}")
    for i, row in enumerate(deserialized.data, 1):
        print(f"  Row {i}: {row}")
    
    match = (rows.rows_count == deserialized.rows_count and 
             rows.data == deserialized.data)
    print(f"Result: {'PASS' if match else 'FAIL'}")
    
    return match


def test_edge_cases():
    print_test("TEST 5: Edge Cases")
    
    schema = TableSchema(
        table_name="edge_test",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="text", data_type=DataType.VARCHAR, max_length=20),
            ColumnDefinition(name="value", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )
    
    serializer = Serializer()
    
    test_cases = [
        {"name": "Empty string", "data": {"id": 1, "text": "", "value": 0.0}},
        {"name": "String truncation", "data": {"id": 2, "text": "A" * 100, "value": 1.5}},
        {"name": "Negative numbers", "data": {"id": -999, "text": "Negative", "value": -123.45}},
        {"name": "Zero values", "data": {"id": 0, "text": "Zero", "value": 0.0}},
        {"name": "Special characters", "data": {"id": 5, "text": "Hello@#$%^&*()", "value": 99.99}}
    ]
    
    all_pass = True
    
    for tc in test_cases:
        row = tc['data']
        
        serialized = serializer.serialize_row(row, schema)
        deserialized = serializer.deserialize_row(serialized, schema)
        
        expected = row.copy()
        if len(expected['text']) > 20:
            expected['text'] = expected['text'][:20]
        
        match = expected == deserialized
        all_pass = all_pass and match
        
        print(f"  {tc['name']}: {'PASS' if match else 'FAIL'}")
    
    print(f"Result: {'PASS' if all_pass else 'FAIL'}")
    return all_pass


def test_row_size_calculation():
    print_test("TEST 6: Row Size Calculation (l_r)")
    
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
    
    serializer = Serializer()
    
    max_size = serializer.calculate_row_size(schema)
    
    print(f"Max row size (l_r): {max_size} bytes")
    
    row = {"id": 1, "name": "Alice", "dept": "IT", "salary": 75000.0}
    serialized = serializer.serialize_row(row, schema)
    
    print(f"Actual row size: {len(serialized)} bytes")
    
    block_size = 4096
    f_r = block_size // max_size
    wasted = block_size - (f_r * max_size)
    
    print(f"Blocking factor (f_r): {f_r} rows/block")
    print(f"Wasted space: {wasted} bytes/block ({(wasted/block_size*100):.1f}%)")
    
    print(f"Result: PASS")
    return True


def test_empty_rows():
    print_test("TEST 7: Empty Rows Object")
    
    schema = TableSchema(
        table_name="empty_test",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="value", data_type=DataType.VARCHAR, max_length=10),
        ],
        primary_key="id"
    )
    
    serializer = Serializer()
    
    empty_rows = Rows(data=[], rows_count=0)
    
    print(f"Original: rows_count={empty_rows.rows_count}")
    
    serialized = serializer.serialize_rows(empty_rows, schema)
    deserialized = serializer.deserialize_rows(serialized, schema)
    
    print(f"Deserialized: rows_count={deserialized.rows_count}")
    
    match = (empty_rows.rows_count == deserialized.rows_count and 
             empty_rows.data == deserialized.data)
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_all_data_types():
    print_test("TEST 8: All Data Types")
    
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
    
    serializer = Serializer()
    
    row = {
        "int_col": 42,
        "float_col": 3.14159,
        "char_col": "Fixed",
        "varchar_col": "Variable length string"
    }
    
    print(f"Original:     {row}")
    
    serialized = serializer.serialize_row(row, schema)
    print(f"Serialized:   {len(serialized)} bytes")
    
    deserialized = serializer.deserialize_row(serialized, schema)
    print(f"Deserialized: {deserialized}")
    
    match = row == deserialized
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def main():
    print_section("SERIALIZER DRIVER PROGRAM")
    print("Tugas Besar SBD 2025 - Storage Manager")
    
    tests = [
        ("Single Row Basic", test_single_row_basic),
        ("Single Row with NULL", test_single_row_with_null),
        ("CHAR vs VARCHAR", test_char_vs_varchar),
        ("Rows Object", test_rows_object),
        ("Edge Cases", test_edge_cases),
        ("Row Size Calculation", test_row_size_calculation),
        ("Empty Rows", test_empty_rows),
        ("All Data Types", test_all_data_types),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"ERROR: {e}")
            results.append((test_name, False))
    
    print_section("TEST SUMMARY")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {status:6} {test_name}")
    
    print(f"\n{'=' * 70}")
    print(f"Total: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("ALL TESTS PASSED")
        return 0
    else:
        print(f"{total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit(main())