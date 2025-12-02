import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.processor import QueryProcessor
from src.optimizer.optimizer import QueryOptimizer
from src.storage.storage_manager import StorageManager
from src.core.models import (
    ExecutionResult,
    TableSchema,
    ColumnDefinition,
    DataType,
    ForeignKeyConstraint,
    ForeignKeyAction,
)


class DummyConcurrencyControlManager:
    def __init__(self):
        self._counter = 0

    def begin_transaction(self) -> int:
        self._counter += 1
        return self._counter

    def end_transaction(self, _transaction_id: int) -> None:
        return None

    def log_object(self, *_args, **_kwargs):
        return None

    def validate_object(self, *_args, **_kwargs):
        return None


class DummyFailureRecoveryManager:
    def write_log(self, *_args, **_kwargs):
        return None

    def recover(self, *_args, **_kwargs):
        return None


TEST_DATA_DIR = "src/data_test"


def cleanup_test_data():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)


def setup_processor_with_fk_chain() -> tuple[QueryProcessor, StorageManager]:
    storage = StorageManager("data_test")
    optimizer = QueryOptimizer(storage_manager=storage)
    ccm = DummyConcurrencyControlManager()
    frm = DummyFailureRecoveryManager()
    processor = QueryProcessor(optimizer, ccm, frm, storage)

    departments = TableSchema(
        table_name="departments",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
        ],
        primary_key="id",
    )

    employees = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(
                name="department_id",
                data_type=DataType.INTEGER,
                foreign_key=ForeignKeyConstraint(
                    referenced_table="departments",
                    referenced_column="id",
                    on_delete=ForeignKeyAction.RESTRICT,
                ),
            ),
        ],
        primary_key="id",
    )

    employee_projects = TableSchema(
        table_name="employee_projects",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(
                name="employee_id",
                data_type=DataType.INTEGER,
                foreign_key=ForeignKeyConstraint(
                    referenced_table="employees",
                    referenced_column="id",
                    on_delete=ForeignKeyAction.RESTRICT,
                ),
            ),
        ],
        primary_key="id",
    )

    storage.create_table(departments)
    storage.create_table(employees)
    storage.create_table(employee_projects)

    return processor, storage


def test_drop_table_restrict_blocks_dependent_tables():
    cleanup_test_data()
    processor, _ = setup_processor_with_fk_chain()

    try:
        with pytest.raises(ValueError) as excinfo:
            processor.execute_query("DROP TABLE departments")

        assert "employees" in str(excinfo.value)
    finally:
        cleanup_test_data()


def test_drop_table_cascade_removes_dependents():
    cleanup_test_data()
    processor, storage = setup_processor_with_fk_chain()

    try:
        result = processor.execute_query("DROP TABLE departments CASCADE")

        assert isinstance(result, ExecutionResult)
        assert "departments" in result.message
        assert storage.list_tables() == []
    finally:
        cleanup_test_data()


def test_drop_table_without_dependents_succeeds():
    cleanup_test_data()
    processor, storage = setup_processor_with_fk_chain()

    try:
        result = processor.execute_query("DROP TABLE employee_projects")

        assert isinstance(result, ExecutionResult)
        remaining_tables = sorted(storage.list_tables())
        assert remaining_tables == ["departments", "employees"]
    finally:
        cleanup_test_data()
