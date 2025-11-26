import os
import shutil

import pytest

from src.concurrency.concurrency_manager import ConcurrencyControlManager
from src.core.models import ColumnDefinition, DataType, Rows, TableSchema
from src.failure.failure_recovery_manager import FailureRecoveryManager
from src.optimizer.optimizer import QueryOptimizer
from src.processor.processor import QueryProcessor
from src.storage.storage_manager import StorageManager

DATA_DIR = "data_join_tests"
DATA_PATH = os.path.join("src", DATA_DIR)


def _cleanup_data_dir() -> None:
    shutil.rmtree(DATA_PATH, ignore_errors=True)


def _seed_join_tables(storage: StorageManager) -> None:
    departments_schema = TableSchema(
        table_name="departments",
        primary_key="department_id",
        columns=[
            ColumnDefinition(name="department_id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="department_name", data_type=DataType.VARCHAR, max_length=40),
            ColumnDefinition(name="region", data_type=DataType.VARCHAR, max_length=20),
        ],
    )
    employees_schema = TableSchema(
        table_name="employees",
        primary_key="employee_id",
        columns=[
            ColumnDefinition(name="employee_id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="employee_name", data_type=DataType.VARCHAR, max_length=60),
            ColumnDefinition(name="department_id", data_type=DataType.INTEGER),
        ],
    )
    projects_schema = TableSchema(
        table_name="projects",
        primary_key="project_id",
        columns=[
            ColumnDefinition(name="project_id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="department_id", data_type=DataType.INTEGER),
            ColumnDefinition(name="project_name", data_type=DataType.VARCHAR, max_length=80),
            ColumnDefinition(name="project_status", data_type=DataType.VARCHAR, max_length=20),
        ],
    )
    assignments_schema = TableSchema(
        table_name="assignments",
        primary_key="assignment_id",
        columns=[
            ColumnDefinition(name="assignment_id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="project_id", data_type=DataType.INTEGER),
            ColumnDefinition(name="employee_id", data_type=DataType.INTEGER),
            ColumnDefinition(name="department_id", data_type=DataType.INTEGER),
        ],
    )

    datasets = [
        (
            departments_schema,
            [
                {"department_id": 1, "department_name": "Platform", "region": "NA"},
                {"department_id": 2, "department_name": "Sales", "region": "EMEA"},
                {"department_id": 3, "department_name": "Research", "region": "APAC"},
            ],
        ),
        (
            employees_schema,
            [
                {"employee_id": 101, "employee_name": "Alex Rivera", "department_id": 1},
                {"employee_id": 102, "employee_name": "Brooke Li", "department_id": 1},
                {"employee_id": 201, "employee_name": "Casey Patel", "department_id": 2},
                {"employee_id": 301, "employee_name": "Drew Ito", "department_id": 3},
            ],
        ),
        (
            projects_schema,
            [
                {"project_id": 1001, "department_id": 1, "project_name": "Telemetry Refresh", "project_status": "active"},
                {"project_id": 2001, "department_id": 2, "project_name": "Enterprise Rollout", "project_status": "active"},
                {"project_id": 3001, "department_id": 3, "project_name": "Adaptive Modeling", "project_status": "on_hold"},
            ],
        ),
        (
            assignments_schema,
            [
                {"assignment_id": 1, "project_id": 1001, "employee_id": 101, "department_id": 1},
                {"assignment_id": 2, "project_id": 1001, "employee_id": 102, "department_id": 1},
                {"assignment_id": 3, "project_id": 2001, "employee_id": 201, "department_id": 2},
                {"assignment_id": 4, "project_id": 3001, "employee_id": 301, "department_id": 3},
            ],
        ),
    ]

    for schema, rows in datasets:
        if storage.get_table_schema(schema.table_name):
            storage.drop_table(schema.table_name)

        storage.create_table(schema)
        storage.dml_manager.save_all_rows(
            schema.table_name,
            Rows(data=list(rows), rows_count=len(rows)),
            schema,
        )


@pytest.fixture()
def processor():
    _cleanup_data_dir()
    storage = StorageManager(DATA_DIR)
    optimizer = QueryOptimizer(storage_manager=storage)
    concurrency = ConcurrencyControlManager("Timestamp")
    failure = FailureRecoveryManager()
    _seed_join_tables(storage)
    qp = QueryProcessor(optimizer, concurrency, failure, storage)
    try:
        yield qp
    finally:
        _cleanup_data_dir()


def test_inner_join_returns_expected_rows(processor: QueryProcessor):
    query = (
        "SELECT employees.employee_name, departments.department_name "
        "FROM employees JOIN departments "
        "ON employees.department_id = departments.department_id "
        "ORDER BY employees.employee_id"
    )

    result = processor.execute_query(query)

    assert result.data is not None
    assert result.data.rows_count == 4
    names = [row["employees.employee_name"] for row in result.data.data]
    departments = [row["departments.department_name"] for row in result.data.data]

    assert names == ["Alex Rivera", "Brooke Li", "Casey Patel", "Drew Ito"]
    assert departments == ["Platform", "Platform", "Sales", "Research"]


def test_natural_join_deduplicates_shared_columns(processor: QueryProcessor):
    query = (
        "SELECT department_id, project_name, region "
        "FROM projects NATURAL JOIN departments "
        "ORDER BY project_id"
    )

    result = processor.execute_query(query)

    assert result.data is not None
    assert result.data.rows_count == 3

    project_names = [row["projects.project_name"] for row in result.data.data]
    regions = [row["departments.region"] for row in result.data.data]
    dept_ids = [row["projects.department_id"] for row in result.data.data]

    assert project_names == [
        "Telemetry Refresh",
        "Enterprise Rollout",
        "Adaptive Modeling",
    ]
    assert regions == ["NA", "EMEA", "APAC"]
    assert dept_ids == [1, 2, 3]
    assert all("departments.department_id" not in row for row in result.data.data)


def test_chained_join_across_multiple_tables(processor: QueryProcessor):
    query = (
        "SELECT assignments.assignment_id, projects.project_name, employees.employee_name, departments.region "
        "FROM assignments "
        "JOIN projects ON assignments.project_id = projects.project_id "
        "JOIN departments ON projects.department_id = departments.department_id "
        "JOIN employees ON assignments.employee_id = employees.employee_id "
        "ORDER BY assignments.assignment_id"
    )

    result = processor.execute_query(query)

    assert result.data is not None
    assert result.data.rows_count == 4

    tuples = [
        (
            row["assignments.assignment_id"],
            row["projects.project_name"],
            row["employees.employee_name"],
            row["departments.region"],
        )
        for row in result.data.data
    ]

    assert tuples == [
        (1, "Telemetry Refresh", "Alex Rivera", "NA"),
        (2, "Telemetry Refresh", "Brooke Li", "NA"),
        (3, "Enterprise Rollout", "Casey Patel", "EMEA"),
        (4, "Adaptive Modeling", "Drew Ito", "APAC"),
    ]
