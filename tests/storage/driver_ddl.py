import sys
import os
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.storage.storage_manager import StorageManager
from src.core.models import (
    TableSchema,
    ColumnDefinition,
    DataType,
    ForeignKeyConstraint,
    ForeignKeyAction
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


def test_create_table_basic():
    print_test("TEST 1: Create Table")

    storage = StorageManager("data_test")

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
    
    print(f"Table created: {schema.table_name}")
    print(f"Columns: {len(schema.columns)}")
    print(f"Retrieved: {retrieved.table_name}")
    print(f"Retrieved columns: {len(retrieved.columns)}")
    
    match = (schema.table_name == retrieved.table_name and
             len(schema.columns) == len(retrieved.columns))
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_create_table_duplicate():
    print_test("TEST 2: Create Table - Duplicate")
    
    storage = StorageManager("data_test")
    
    schema = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
        ],
        primary_key="id"
    )
    
    try:
        storage.create_table(schema)
        print("Result: FAIL (Should have raised error)")
        return False
    except ValueError as e:
        print(f"Error caught: {e}")
        print("Result: PASS")
        return True


def test_drop_table():
    print_test("TEST 3: Drop Table")

    storage = StorageManager("data_test")

    tables_before = storage.list_tables()
    print(f"Tables before drop: {tables_before}")
    
    storage.drop_table("employees")
    
    tables_after = storage.list_tables()
    print(f"Tables after drop: {tables_after}")
    
    match = "employees" not in tables_after
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_list_tables():
    print_test("TEST 4: List Tables")

    storage = StorageManager("data_test")

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
    print(f"Tables created: {len(schemas)}")
    print(f"Tables found: {len(tables)}")
    print(f"Table names: {sorted(tables)}")
    
    match = len(tables) == 3 and all(s.table_name in tables for s in schemas)
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_create_table_with_foreign_key():
    print_test("TEST 5: Create Table - With Foreign Key")

    storage = StorageManager("data_test")

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
    
    print(f"Table created: {orders_schema.table_name}")
    print(f"Has FK: {fk is not None}")
    if fk:
        print(f"FK refs: {fk.referenced_table}.{fk.referenced_column}")
        print(f"ON DELETE: {fk.on_delete.value}")
    
    match = (fk is not None and
             fk.referenced_table == "users" and
             fk.referenced_column == "id")
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_get_nonexistent_schema():
    print_test("TEST 6: Get Schema - Nonexistent Table")

    storage = StorageManager("data_test")

    schema = storage.get_table_schema("nonexistent_table")
    
    print(f"Schema returned: {schema}")
    
    match = schema is None
    
    print(f"Result: {'PASS' if match else 'FAIL'}")
    return match


def test_schema_validation():
    print_test("TEST 7: Schema Validation")

    storage = StorageManager("data_test")

    invalid_schemas = [
        ("Empty table name", TableSchema(table_name="", columns=[], primary_key=None)),
        ("No columns", TableSchema(table_name="test", columns=[], primary_key=None)),
        ("Duplicate columns", TableSchema(
            table_name="test",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER),
                ColumnDefinition(name="id", data_type=DataType.INTEGER),
            ],
            primary_key=None
        )),
    ]
    
    all_pass = True
    
    for desc, schema in invalid_schemas:
        try:
            storage.create_table(schema)
            print(f"  {desc}: FAIL (Should have raised error)")
            all_pass = False
        except ValueError:
            print(f"  {desc}: PASS")
    
    print(f"Result: {'PASS' if all_pass else 'FAIL'}")
    return all_pass


def main():
    print_section("DDL OPERATIONS TEST")
    cleanup_test_data()
    
    tests = [
        ("Create Table Basic", test_create_table_basic),
        ("Create Table Duplicate", test_create_table_duplicate),
        ("Drop Table", test_drop_table),
        ("List Tables", test_list_tables),
        ("Create Table with FK", test_create_table_with_foreign_key),
        ("Get Nonexistent Schema", test_get_nonexistent_schema),
        ("Schema Validation", test_schema_validation),
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
    
    cleanup_test_data()


if __name__ == "__main__":
    exit(main())