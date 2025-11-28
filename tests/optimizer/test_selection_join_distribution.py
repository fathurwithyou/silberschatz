import pytest
from unittest.mock import MagicMock
from src.optimizer.rules.selection.join_distribution import SelectionJoinDistributionRule
from src.core.models.query import QueryTree, QueryNodeType

@pytest.fixture
def rule():
    mock_storage_manager = MagicMock()
    mock_storage_manager.get_table_schema.return_value = None
    return SelectionJoinDistributionRule(storage_manager=mock_storage_manager)


def test_rule_7a_left_only(rule):
    # LEFT & RIGHT tables
    left = QueryTree(type=QueryNodeType.TABLE, value="employees", children=[], parent=None)
    right = QueryTree(type=QueryNodeType.TABLE, value="departments", children=[], parent=None)

    # JOIN Node
    join_node = QueryTree(
        type=QueryNodeType.JOIN,
        value="employees.dept_id = departments.id",
        children=[left, right],
        parent=None
    )

    node = QueryTree(
        type=QueryNodeType.SELECTION,
        value="employees.age > 30",
        children=[join_node],
        parent=None
    )

    # Rule must apply
    assert rule.is_applicable(node)

    new_tree = rule.apply(node)
    assert new_tree is not None
    assert new_tree.type == QueryNodeType.JOIN

    new_left = new_tree.children[0]
    new_right = new_tree.children[1]

    # LEFT becomes selection
    assert new_left.type == QueryNodeType.SELECTION
    assert new_left.value == "employees.age > 30"
    assert new_left.children[0].value == "employees"

    # RIGHT remains table
    assert new_right.value == "departments"


def test_rule_7b_left_and_right(rule):
    # LEFT & RIGHT tables
    left = QueryTree(type=QueryNodeType.TABLE, value="employees", children=[], parent=None)
    right = QueryTree(type=QueryNodeType.TABLE, value="departments", children=[], parent=None)

    # JOIN node
    join_node = QueryTree(
        type=QueryNodeType.JOIN,
        value="employees.dept_id = departments.id",
        children=[left, right],
        parent=None
    )

    # Condition consists of 2 parts:
    # employees.age > 30 → LEFT
    # departments.name = 'IT' → RIGHT
    node = QueryTree(
        type=QueryNodeType.SELECTION,
        value="employees.age > 30 AND departments.name = 'IT'",
        children=[join_node],
        parent=None
    )

    assert rule.is_applicable(node)
    new_tree = rule.apply(node)
    assert new_tree is not None

    # Root should still be JOIN
    assert new_tree.type == QueryNodeType.JOIN

    new_left = new_tree.children[0]
    new_right = new_tree.children[1]

    # LEFT side should get employees.age > 30
    assert new_left.type == QueryNodeType.SELECTION
    assert new_left.value == "employees.age > 30"
    assert new_left.children[0].value == "employees"

    # RIGHT side should get departments.name = 'IT'
    assert new_right.type == QueryNodeType.SELECTION
    assert new_right.value == "departments.name = 'IT'"
    assert new_right.children[0].value == "departments"
