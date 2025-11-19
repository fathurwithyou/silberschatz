import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.operators.selection_operator import SelectionOperator
from src.core.models.result import Rows
from src.core.models.storage import TableSchema, ColumnDefinition, DataType


def create_test_schema():
    """Create a test table schema for users."""
    columns = [
        ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
        ColumnDefinition(name="name", data_type=DataType.VARCHAR),
        ColumnDefinition(name="age", data_type=DataType.INTEGER),
        ColumnDefinition(name="salary", data_type=DataType.FLOAT)
    ]
    return [TableSchema(table_name="users", columns=columns)]


def create_test_data():
    """Create test data for users table."""
    return [
        {"users.id": 1, "users.name": "John", "users.age": 25, "users.salary": 50000.0},
        {"users.id": 2, "users.name": "Jane", "users.age": 30, "users.salary": 60000.0},
        {"users.id": 3, "users.name": "Bob", "users.age": 35, "users.salary": 70000.0},
        {"users.id": 4, "users.name": "Alice", "users.age": 28, "users.salary": 55000.0},
        {"users.id": 5, "users.name": "Charlie", "users.age": 22, "users.salary": 45000.0}
    ]


def test_selection_basic_equality():
    """Test basic equality selection condition."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.name = 'John'")
    
    assert result.rows_count == 1
    assert len(result.data) == 1
    assert result.data[0]["users.name"] == "John"
    assert result.schema == schema


def test_selection_numeric_comparison():
    """Test numeric comparison conditions."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.age > 28")
    
    assert result.rows_count == 2  # Jane (30) and Bob (35)
    assert len(result.data) == 2
    ages = [row["users.age"] for row in result.data]
    assert all(age > 28 for age in ages)


def test_selection_less_than_condition():
    """Test less than condition."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.age < 28")
    
    assert result.rows_count == 2  # John (25) and Charlie (22)
    ages = [row["users.age"] for row in result.data]
    assert all(age < 28 for age in ages)


def test_selection_greater_equal_condition():
    """Test greater than or equal condition."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.salary >= 55000")
    
    assert result.rows_count == 3  # Jane, Bob, Alice
    salaries = [row["users.salary"] for row in result.data]
    assert all(salary >= 55000 for salary in salaries)


def test_selection_and_condition():
    """Test AND condition."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.age > 25 AND users.salary > 55000")
    
    assert result.rows_count == 2  # Jane and Bob
    for row in result.data:
        assert row["users.age"] > 25
        assert row["users.salary"] > 55000


def test_selection_or_condition():
    """Test OR condition."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.age < 25 OR users.salary > 65000")
    
    assert result.rows_count == 2  # Charlie (age 22) and Bob (salary 70000)
    for row in result.data:
        assert row["users.age"] < 25 or row["users.salary"] > 65000


def test_selection_no_matches():
    """Test selection with no matching records."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.age > 100")
    
    assert result.rows_count == 0
    assert len(result.data) == 0
    assert result.schema == schema


def test_selection_all_matches():
    """Test selection where all records match."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.id > 0")
    
    assert result.rows_count == 5
    assert len(result.data) == 5
    assert result.data == data
    assert result.schema == schema


def test_selection_empty_input():
    """Test selection with empty input data."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = []
    
    rows = Rows(data=data, rows_count=0, schema=schema)
    
    result = operator.execute(rows, "users.name = 'John'")
    
    assert result.rows_count == 0
    assert len(result.data) == 0
    assert result.schema == schema


def test_selection_not_equal():
    """Test not equal condition."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "users.name != 'John'")
    
    assert result.rows_count == 4  # Everyone except John
    names = [row["users.name"] for row in result.data]
    assert "John" not in names


def test_selection_complex_condition():
    """Test complex condition with parentheses."""
    operator = SelectionOperator()
    schema = create_test_schema()
    data = create_test_data()
    
    rows = Rows(data=data, rows_count=len(data), schema=schema)
    
    result = operator.execute(rows, "(users.age < 30 AND users.salary > 45000) OR users.name = 'Bob'")
    
    assert result.rows_count >= 1  # Should include at least Bob
    
    for row in result.data:
        condition1 = row["users.age"] < 30 and row["users.salary"] > 45000
        condition2 = row["users.name"] == "Bob"
        assert condition1 or condition2


if __name__ == "__main__":
    test_selection_basic_equality()
    test_selection_numeric_comparison()
    test_selection_less_than_condition()
    test_selection_greater_equal_condition()
    test_selection_and_condition()
    test_selection_or_condition()
    test_selection_no_matches()
    test_selection_all_matches()
    test_selection_empty_input()
    test_selection_not_equal()
    test_selection_complex_condition()
    
    print("All selection operator tests passed!")