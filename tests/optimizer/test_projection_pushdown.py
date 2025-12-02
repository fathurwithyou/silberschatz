import pytest
from src.optimizer.rules.projection.pushdown import ProjectionPushdownRule
from src.core.models.query import QueryTree


@pytest.fixture
def rule():
    return ProjectionPushdownRule()


def test_pushdown_through_selection(rule):
    # π ( name, age ) on σ(age > 30)(employees)
    table = QueryTree(type="table", value="employees", children=[], parent=None)

    selection = QueryTree(
        type="selection",
        value="age > 30",
        children=[table],
        parent=None
    )

    proj = QueryTree(
        type="projection",
        value="name, age",
        children=[selection],
        parent=None
    )

    assert rule.is_applicable(proj)
    new_tree = rule.apply(proj)

    assert new_tree.type == "projection"
    assert new_tree.value == "name, age"

    child_sel = new_tree.children[0]
    assert child_sel.type == "selection"

    pushed_proj = child_sel.children[0]
    assert pushed_proj.type == "projection"
    assert "age" in pushed_proj.value
    assert "name" in pushed_proj.value

    leaf = pushed_proj.children[0]
    assert leaf.type == "table"
    assert leaf.value == "employees"


def test_pushdown_through_order_by(rule):
    # π(name) on ORDER BY age (employees)
    table = QueryTree("table", "employees", [])
    order_node = QueryTree("order_by", "age DESC", [table])
    proj = QueryTree("projection", "name", [order_node])

    new_tree = rule.apply(proj)

    assert new_tree.type == "projection"

    child_order = new_tree.children[0]
    assert child_order.type == "order_by"

    pushed_proj = child_order.children[0]
    assert pushed_proj.type == "projection"

    assert "name" in pushed_proj.value
    assert "age" in pushed_proj.value


def test_pushdown_through_limit(rule):
    # π(name) on LIMIT 10 (employees)
    table = QueryTree("table", "employees", [])
    limit = QueryTree("limit", "10", [table])
    proj = QueryTree("projection", "name", [limit])

    new_tree = rule.apply(proj)

    # top still projection
    assert new_tree.type == "projection"

    child_limit = new_tree.children[0]
    assert child_limit.type == "limit"

    pushed_proj = child_limit.children[0]
    assert pushed_proj.type == "projection"
    assert pushed_proj.value == "name"

    assert pushed_proj.children[0].value == "employees"


def test_pushdown_through_join_split(rule):
    # SELECT name FROM employees JOIN departments ON employees.dept_id = departments.id
    left = QueryTree("table", "employees", [])
    right = QueryTree("table", "departments", [])

    join = QueryTree(
        type="join",
        value="employees.dept_id = departments.id",
        children=[left, right],
        parent=None
    )

    proj = QueryTree(
        type="projection",
        value="employees.name",
        children=[join],
        parent=None
    )

    assert rule.is_applicable(proj)
    new_tree = rule.apply(proj)

    assert new_tree.type == "projection"

    updated_join = new_tree.children[0]
    assert updated_join.type == "join"

    new_left = updated_join.children[0]
    assert new_left.type == "projection"
    assert "employees.name" in new_left.value
    assert "employees.dept_id" in new_left.value

    new_right = updated_join.children[1]
    assert new_right.type == "projection"
    assert "departments.id" in new_right.value

    assert new_left.children[0].value == "employees"
    assert new_right.children[0].value == "departments"
