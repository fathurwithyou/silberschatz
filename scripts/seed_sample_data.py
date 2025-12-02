"""
Script to seed sample data into the storage system for testing purposes.
"""
from __future__ import annotations

import argparse
from typing import Iterable, List

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.storage.storage_manager import StorageManager
from src.core.models import (
    TableSchema,
    ColumnDefinition,
    DataType,
    Rows,
)


def seed_table(storage: StorageManager, schema: TableSchema, rows: Rows) -> None:
    existing = storage.get_table_schema(schema.table_name)
    if existing:
        storage.drop_table(schema.table_name)

    storage.create_table(schema)
    storage.dml_manager.save_all_rows(schema.table_name, rows, schema)
    print(f"Seeded table '{schema.table_name}' with {rows.rows_count} rows.")


def build_rows(data: Iterable[dict]) -> Rows:
    data_list = list(data)
    return Rows(data=data_list, rows_count=len(data_list))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data_test")
    parser.add_argument("--table", action="append", dest="tables")
    args = parser.parse_args()

    storage = StorageManager(args.data_dir)

    tables_to_seed: List[str] = args.tables or ["users", "orders", "departments"]
    if "users" in tables_to_seed:
        schema = TableSchema(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="email", data_type=DataType.VARCHAR, max_length=100),
                ColumnDefinition(name="department_id", data_type=DataType.INTEGER),
            ],
            primary_key="id",
        )
        rows = build_rows(
            [
                {"id": 1, "name": "Alice", "email": "alice@example.com", "department_id": 10},
                {"id": 2, "name": "Bob", "email": "bob@example.com", "department_id": 20},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com", "department_id": 10},
                {"id": 4, "name": "Diana", "email": "diana@example.com", "department_id": 30},
            ]
        )
        seed_table(storage, schema, rows)

    if "orders" in tables_to_seed:
        schema = TableSchema(
            table_name="orders",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="user_id", data_type=DataType.INTEGER),
                ColumnDefinition(name="amount", data_type=DataType.FLOAT),
                ColumnDefinition(name="status", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        )
        rows = build_rows(
            [
                {"id": 101, "user_id": 1, "amount": 250.0, "status": "shipped"},
                {"id": 102, "user_id": 2, "amount": 120.5, "status": "processing"},
                {"id": 103, "user_id": 1, "amount": 75.25, "status": "cancelled"},
                {"id": 104, "user_id": 3, "amount": 460.0, "status": "shipped"},
            ]
        )
        seed_table(storage, schema, rows)

    if "departments" in tables_to_seed:
        schema = TableSchema(
            table_name="departments",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ],
            primary_key="id",
        )
        rows = build_rows(
            [
                {"id": 10, "name": "Engineering"},
                {"id": 20, "name": "Sales"},
                {"id": 30, "name": "Marketing"},
            ]
        )
        seed_table(storage, schema, rows)

    print(f"Data directory '{args.data_dir}' seeded tables: {', '.join(tables_to_seed)}.")


if __name__ == "__main__":
    main()
