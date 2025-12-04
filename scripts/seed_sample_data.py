"""
Script to seed sample data into the storage system for testing purposes.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, Iterable, List, Sequence

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.storage.storage_manager import StorageManager  # noqa: E402
from src.core.models import (  # noqa: E402
    ColumnDefinition,
    DataType,
    ForeignKeyAction,
    ForeignKeyConstraint,
    Rows,
    TableSchema,
)

RANDOM_SEED = 1337


@dataclass
class TableSeed:
    schema: TableSchema
    rows: Rows


@dataclass(frozen=True)
class SeederPreset:
    default_tables: Sequence[str]
    builder: Callable[[random.Random], Dict[str, TableSeed]]


def seed_table(storage: StorageManager, payload: TableSeed) -> None:
    existing = storage.get_table_schema(payload.schema.table_name)
    if existing:
        storage.drop_table(payload.schema.table_name)

    storage.create_table(payload.schema)
    storage.dml_manager.save_all_rows(payload.schema.table_name, payload.rows, payload.schema)
    print(
        f"Seeded table '{payload.schema.table_name}' with {payload.rows.rows_count} rows."
    )


def build_rows(data: Iterable[dict]) -> Rows:
    data_list = list(data)
    return Rows(data=data_list, rows_count=len(data_list))


def build_mini_seed_payloads(rng: random.Random) -> Dict[str, TableSeed]:
    schemas = {
        "users": TableSchema(
            table_name="users",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="email", data_type=DataType.VARCHAR, max_length=100),
                ColumnDefinition(name="department_id", data_type=DataType.INTEGER),
            ],
            primary_key="id",
        ),
        "orders": TableSchema(
            table_name="orders",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="user_id", data_type=DataType.INTEGER),
                ColumnDefinition(name="amount", data_type=DataType.FLOAT),
                ColumnDefinition(name="status", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        ),
        "departments": TableSchema(
            table_name="departments",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ],
            primary_key="id",
        ),
    }

    users = build_rows(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "department_id": 10},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "department_id": 20},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "department_id": 10},
            {"id": 4, "name": "Diana", "email": "diana@example.com", "department_id": 30},
        ]
    )

    orders = build_rows(
        [
            {"id": 101, "user_id": 1, "amount": 250.0, "status": "shipped"},
            {"id": 102, "user_id": 2, "amount": 120.5, "status": "processing"},
            {"id": 103, "user_id": 1, "amount": 75.25, "status": "cancelled"},
            {"id": 104, "user_id": 3, "amount": 460.0, "status": "shipped"},
        ]
    )

    departments = build_rows(
        [
            {"id": 10, "name": "Engineering"},
            {"id": 20, "name": "Sales"},
            {"id": 30, "name": "Marketing"},
        ]
    )

    return {
        "users": TableSeed(schemas["users"], users),
        "orders": TableSeed(schemas["orders"], orders),
        "departments": TableSeed(schemas["departments"], departments),
    }


def build_full_seed_payloads(rng: random.Random) -> Dict[str, TableSeed]:
    schemas = {
        "departments": TableSchema(
            table_name="departments",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=80),
                ColumnDefinition(name="location", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="budget", data_type=DataType.FLOAT),
                ColumnDefinition(name="cost_center", data_type=DataType.CHAR, max_length=10),
            ],
            primary_key="id",
        ),
        "teams": TableSchema(
            table_name="teams",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="department_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("departments", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=80),
                ColumnDefinition(name="focus_area", data_type=DataType.VARCHAR, max_length=80),
            ],
            primary_key="id",
        ),
        "roles": TableSchema(
            table_name="roles",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
                ColumnDefinition(name="permission_level", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        ),
        "employees": TableSchema(
            table_name="employees",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="department_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("departments", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(
                    name="team_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("teams", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=100),
                ColumnDefinition(name="email", data_type=DataType.VARCHAR, max_length=120),
                ColumnDefinition(name="salary", data_type=DataType.FLOAT),
                ColumnDefinition(name="status", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="hired_on", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="title", data_type=DataType.VARCHAR, max_length=60),
            ],
            primary_key="id",
        ),
        "employee_roles": TableSchema(
            table_name="employee_roles",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="employee_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(
                    name="role_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("roles", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="assigned_on", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        ),
        "projects": TableSchema(
            table_name="projects",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="department_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("departments", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=120),
                ColumnDefinition(name="budget", data_type=DataType.FLOAT),
                ColumnDefinition(name="status", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="start_date", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="due_date", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        ),
        "project_assignments": TableSchema(
            table_name="project_assignments",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="project_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("projects", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(
                    name="employee_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="allocation_percent", data_type=DataType.INTEGER),
                ColumnDefinition(name="is_lead", data_type=DataType.CHAR, max_length=1),
            ],
            primary_key="id",
        ),
        "products": TableSchema(
            table_name="products",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=120),
                ColumnDefinition(name="category", data_type=DataType.VARCHAR, max_length=60),
                ColumnDefinition(name="price", data_type=DataType.FLOAT),
                ColumnDefinition(name="sku", data_type=DataType.CHAR, max_length=16),
                ColumnDefinition(name="stock", data_type=DataType.INTEGER),
            ],
            primary_key="id",
        ),
        "orders": TableSchema(
            table_name="orders",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="employee_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="order_date", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="total_amount", data_type=DataType.FLOAT),
                ColumnDefinition(name="status", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        ),
        "order_items": TableSchema(
            table_name="order_items",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="order_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("orders", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(
                    name="product_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("products", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="quantity", data_type=DataType.INTEGER),
                ColumnDefinition(name="unit_price", data_type=DataType.FLOAT),
            ],
            primary_key="id",
        ),
        "payments": TableSchema(
            table_name="payments",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="order_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("orders", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="paid_on", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="method", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="amount", data_type=DataType.FLOAT),
            ],
            primary_key="id",
        ),
        "inventory_movements": TableSchema(
            table_name="inventory_movements",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="product_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("products", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="change_type", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="quantity", data_type=DataType.INTEGER),
                ColumnDefinition(name="reference", data_type=DataType.VARCHAR, max_length=80),
                ColumnDefinition(name="occurred_on", data_type=DataType.VARCHAR, max_length=20),
            ],
            primary_key="id",
        ),
        "support_tickets": TableSchema(
            table_name="support_tickets",
            columns=[
                ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
                ColumnDefinition(
                    name="employee_id",
                    data_type=DataType.INTEGER,
                    foreign_key=ForeignKeyConstraint("employees", "id", ForeignKeyAction.CASCADE),
                ),
                ColumnDefinition(name="priority", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="status", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="opened_on", data_type=DataType.VARCHAR, max_length=20),
                ColumnDefinition(name="subject", data_type=DataType.VARCHAR, max_length=120),
            ],
            primary_key="id",
        ),
    }

    departments = []
    locations = ["New York", "Singapore", "Berlin", "Jakarta", "Sydney"]
    for idx, (name, base_budget) in enumerate(
        [
            ("Platform", 2_500_000),
            ("Data Science", 1_600_000),
            ("Growth", 1_200_000),
            ("Operations", 1_400_000),
            ("Customer Success", 900_000),
        ],
        start=1,
    ):
        departments.append(
            {
                "id": idx,
                "name": name,
                "location": rng.choice(locations),
                "budget": round(base_budget * rng.uniform(0.85, 1.2), 2),
                "cost_center": f"CC{idx:03d}",
            }
        )

    teams = []
    team_id = 100
    focus_areas = ["Analytics", "Platform", "Automation", "Payments", "Infra", "Mobile"]
    for dept in departments:
        for focus in rng.sample(focus_areas, k=3):
            teams.append(
                {
                    "id": team_id,
                    "department_id": dept["id"],
                    "name": f"{dept['name']} {focus}",
                    "focus_area": focus,
                }
            )
            team_id += 1

    roles = build_rows(
        [
            {"id": 1, "name": "Engineer", "permission_level": "standard"},
            {"id": 2, "name": "Manager", "permission_level": "elevated"},
            {"id": 3, "name": "Analyst", "permission_level": "standard"},
            {"id": 4, "name": "Director", "permission_level": "admin"},
            {"id": 5, "name": "Support", "permission_level": "standard"},
        ]
    )

    first_names = [
        "Alice",
        "Bob",
        "Charlie",
        "Diana",
        "Ethan",
        "Farah",
        "Grace",
        "Hadi",
        "Indira",
        "Joko",
        "Kara",
        "Liam",
        "Maya",
        "Noor",
        "Omar",
        "Putri",
        "Quincy",
        "Raka",
        "Sari",
        "Tian",
    ]
    last_names = ["Tan", "Wijaya", "Hartono", "Pratama", "Siregar", "Chen", "Rahman", "Ng"]
    titles = ["Senior Engineer", "Engineer", "Data Analyst", "Product Manager", "Designer", "Lead"]
    statuses = ["active", "active", "active", "on_leave", "probation"]

    employees = []
    team_to_employees: Dict[int, List[int]] = defaultdict(list)
    dept_to_employees: Dict[int, List[int]] = defaultdict(list)
    employee_id = 1000
    today = datetime(2025, 1, 1)
    for team in teams:
        members = rng.randint(6, 10)
        for _ in range(members):
            fname = rng.choice(first_names)
            lname = rng.choice(last_names)
            email = f"{fname.lower()}.{lname.lower()}{employee_id}@example.com"
            hire_delta = timedelta(days=rng.randint(90, 2200))
            employees.append(
                {
                    "id": employee_id,
                    "department_id": team["department_id"],
                    "team_id": team["id"],
                    "name": f"{fname} {lname}",
                    "email": email,
                    "salary": round(rng.uniform(45_000, 140_000), 2),
                    "status": rng.choice(statuses),
                    "hired_on": (today - hire_delta).strftime("%Y-%m-%d"),
                    "title": rng.choice(titles),
                }
            )
            team_to_employees[team["id"]].append(employee_id)
            dept_to_employees[team["department_id"]].append(employee_id)
            employee_id += 1

    employee_roles = []
    er_id = 1
    for emp in employees:
        assigned_roles = rng.sample([1, 2, 3, 4, 5], k=rng.randint(1, 3))
        for role_id in assigned_roles:
            employee_roles.append(
                {
                    "id": er_id,
                    "employee_id": emp["id"],
                    "role_id": role_id,
                    "assigned_on": emp["hired_on"],
                }
            )
            er_id += 1

    projects = []
    project_id = 5000
    for dept in departments:
        for idx in range(3):
            start = today - timedelta(days=rng.randint(30, 420))
            duration = timedelta(days=rng.randint(60, 300))
            projects.append(
                {
                    "id": project_id,
                    "department_id": dept["id"],
                    "name": f"{dept['name']} Initiative {idx + 1}",
                    "budget": round(rng.uniform(150_000, 750_000), 2),
                    "status": rng.choice(["planning", "active", "delayed", "completed"]),
                    "start_date": start.strftime("%Y-%m-%d"),
                    "due_date": (start + duration).strftime("%Y-%m-%d"),
                }
            )
            project_id += 1

    project_assignments = []
    assignment_id = 1
    for project in projects:
        candidates = dept_to_employees[project["department_id"]]
        if not candidates:
            continue
        assignees = rng.sample(candidates, k=min(len(candidates), rng.randint(4, 8)))
        lead = rng.choice(assignees)
        for emp_id in assignees:
            project_assignments.append(
                {
                    "id": assignment_id,
                    "project_id": project["id"],
                    "employee_id": emp_id,
                    "allocation_percent": rng.randint(20, 80),
                    "is_lead": "Y" if emp_id == lead else "N",
                }
            )
            assignment_id += 1

    products = []
    product_id = 900
    product_catalog = [
        ("Analytics Suite", "Software"),
        ("Sensor Hub", "Hardware"),
        ("Edge Gateway", "Hardware"),
        ("Cloud Credits", "Service"),
        ("AI Accelerator", "Hardware"),
        ("Support Retainer", "Service"),
        ("Mobile SDK", "Software"),
    ]
    for name, category in product_catalog:
        products.append(
            {
                "id": product_id,
                "name": name,
                "category": category,
                "price": round(rng.uniform(99, 4999), 2),
                "sku": f"SKU{product_id}",
                "stock": rng.randint(25, 400),
            }
        )
        product_id += 1

    orders = []
    order_items = []
    payments = []
    order_id = 7000
    order_item_id = 1
    payment_id = 1
    for _ in range(60):
        employee = rng.choice(employees)
        order_date = today - timedelta(days=rng.randint(5, 180))
        order_products = rng.sample(products, k=rng.randint(1, 4))
        total = 0.0
        for product in order_products:
            quantity = rng.randint(1, 5)
            subtotal = quantity * product["price"]
            total += subtotal
            order_items.append(
                {
                    "id": order_item_id,
                    "order_id": order_id,
                    "product_id": product["id"],
                    "quantity": quantity,
                    "unit_price": product["price"],
                }
            )
            order_item_id += 1

        orders.append(
            {
                "id": order_id,
                "employee_id": employee["id"],
                "order_date": order_date.strftime("%Y-%m-%d"),
                "total_amount": round(total, 2),
                "status": rng.choice(["processing", "shipped", "shipped", "cancelled"]),
            }
        )

        payments.append(
            {
                "id": payment_id,
                "order_id": order_id,
                "paid_on": (order_date + timedelta(days=rng.randint(0, 7))).strftime(
                    "%Y-%m-%d"
                ),
                "method": rng.choice(["bank_transfer", "credit_card", "ewallet"]),
                "amount": round(total, 2),
            }
        )
        payment_id += 1
        order_id += 1

    inventory_movements = []
    movement_id = 1
    for product in products:
        for _ in range(5):
            quantity = rng.randint(5, 50)
            change_type = rng.choice(["restock", "shipment", "adjustment"])
            sign = 1 if change_type == "restock" else -1
            inventory_movements.append(
                {
                    "id": movement_id,
                    "product_id": product["id"],
                    "change_type": change_type,
                    "quantity": sign * quantity,
                    "reference": f"REF{movement_id:05d}",
                    "occurred_on": (
                        today - timedelta(days=rng.randint(1, 120))
                    ).strftime("%Y-%m-%d"),
                }
            )
            movement_id += 1

    support_tickets = []
    ticket_id = 1
    subjects = [
        "VPN access",
        "Laptop issue",
        "Expense approval",
        "Data pipeline",
        "Analytics dashboard",
        "Recruiting request",
    ]
    for _ in range(40):
        employee = rng.choice(employees)
        support_tickets.append(
            {
                "id": ticket_id,
                "employee_id": employee["id"],
                "priority": rng.choice(["low", "medium", "high", "critical"]),
                "status": rng.choice(["open", "open", "in_progress", "resolved"]),
                "opened_on": (
                    today - timedelta(days=rng.randint(1, 60))
                ).strftime("%Y-%m-%d"),
                "subject": rng.choice(subjects),
            }
        )
        ticket_id += 1

    seeds = {
        "departments": TableSeed(schemas["departments"], build_rows(departments)),
        "teams": TableSeed(schemas["teams"], build_rows(teams)),
        "roles": TableSeed(schemas["roles"], roles),
        "employees": TableSeed(schemas["employees"], build_rows(employees)),
        "employee_roles": TableSeed(
            schemas["employee_roles"], build_rows(employee_roles)
        ),
        "projects": TableSeed(schemas["projects"], build_rows(projects)),
        "project_assignments": TableSeed(
            schemas["project_assignments"], build_rows(project_assignments)
        ),
        "products": TableSeed(schemas["products"], build_rows(products)),
        "orders": TableSeed(schemas["orders"], build_rows(orders)),
        "order_items": TableSeed(schemas["order_items"], build_rows(order_items)),
        "payments": TableSeed(schemas["payments"], build_rows(payments)),
        "inventory_movements": TableSeed(
            schemas["inventory_movements"], build_rows(inventory_movements)
        ),
        "support_tickets": TableSeed(
            schemas["support_tickets"], build_rows(support_tickets)
        ),
    }

    return seeds


PRESETS: Dict[str, SeederPreset] = {
    "mini": SeederPreset(
        default_tables=("users", "orders", "departments"),
        builder=build_mini_seed_payloads,
    ),
    "full": SeederPreset(
        default_tables=(
            "departments",
            "teams",
            "roles",
            "employees",
            "employee_roles",
            "projects",
            "project_assignments",
            "products",
            "orders",
            "order_items",
            "payments",
            "inventory_movements",
            "support_tickets",
        ),
        builder=build_full_seed_payloads,
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed storage with deterministic data")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--preset",
        choices=sorted(PRESETS.keys()),
        default="full",
        help="Dataset preset to seed",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Specific table(s) to seed (overrides preset order)",
    )
    args = parser.parse_args()

    storage = StorageManager(args.data_dir)
    rng = random.Random(RANDOM_SEED)
    preset = PRESETS[args.preset]
    payloads = preset.builder(rng)

    tables_to_seed: List[str] = (
        args.tables if args.tables else list(preset.default_tables)
    )

    for table_name in tables_to_seed:
        payload = payloads.get(table_name)
        if not payload:
            print(f"Skipping unknown table '{table_name}'.")
            continue
        seed_table(storage, payload)

    print(
        f"Data directory '{args.data_dir}' seeded tables: {', '.join(tables_to_seed)}."
    )


if __name__ == "__main__":
    main()
