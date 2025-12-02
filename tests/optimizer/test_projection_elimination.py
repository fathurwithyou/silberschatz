import pytest
from src.optimizer.rules.projection.elimination import ProjectionEliminationRule
from src.core.models.query import QueryTree


@pytest.fixture
def rule():
    return ProjectionEliminationRule()


def test_intersection_kept_single_projection(rule):
    # π(a,b) ( π(b,c) (R) )  ->  π(b) (R)
    table = QueryTree(type="table", value="employees", children=[], parent=None)
    inner = QueryTree(type="projection", value="b,c", children=[table], parent=None)
    outer = QueryTree(type="projection", value="a,b", children=[inner], parent=None)

    assert rule.is_applicable(outer)
    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == "projection"
    assert set(map(str.strip, new_tree.value.split(","))) == {"b"}
    assert new_tree.children[0].type == "table"
    assert new_tree.children[0].value == "employees"


def test_outer_redundant_when_intersection_equals_inner(rule):
    # π(a,b,c) ( π(b,c) (R) )  ->  π(b,c) (R)   (outer redundant)
    table = QueryTree("table", "employees", [])
    inner = QueryTree("projection", "b,c", [table], None)
    outer = QueryTree("projection", "a,b,c", [inner], None)

    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == "projection"
    assert set(new_tree.value.split(",")) == {"b", "c"}
    assert new_tree is inner
    assert new_tree.children[0] is table


def test_inner_redundant_when_intersection_equals_outer(rule):
    # π(b) ( π(b,c) (R) )  ->  π(b) (R)   (inner redundant)
    table = QueryTree("table", "employees", [])
    inner = QueryTree("projection", "b,c", [table], None)
    outer = QueryTree("projection", "b", [inner], None)

    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == "projection"
    assert set(new_tree.value.split(",")) == {"b"}
    assert new_tree.children[0] is table


def test_inner_wildcard_outer_specific(rule):
    # π(a,b) ( π(*) (R) )  ->  π(a,b) (R)   (inner is redundant)
    table = QueryTree("table", "employees", [])
    inner = QueryTree("projection", "*", [table], None)
    outer = QueryTree("projection", "a,b", [inner], None)

    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == "projection"
    assert set(new_tree.value.split(",")) == {"a", "b"}
    assert new_tree.children[0] is table


def test_outer_wildcard_inner_specific(rule):
    # π(*) ( π(a,b) (R) )  ->  π(a,b) (R)   (outer is redundant)
    table = QueryTree("table", "employees", [])
    inner = QueryTree("projection", "a,b", [table], None)
    outer = QueryTree("projection", "*", [inner], None)

    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == "projection"
    assert set(new_tree.value.split(",")) == {"a", "b"}
    assert new_tree is inner
    assert new_tree.children[0] is table


def test_both_wildcards_collapse_to_single(rule):
    # π(*) ( π(*) (R) )  ->  π(*) (R)
    table = QueryTree("table", "employees", [])
    inner = QueryTree("projection", "*", [table], None)
    outer = QueryTree("projection", "*", [inner], None)

    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == "projection"
    assert new_tree.value == "*"
    assert new_tree.children[0] is table


def test_noop_when_child_not_projection(rule):
    # π(a) ( σ(x > 1) (R) )  ->  no change (rule returns None)
    table = QueryTree("table", "employees", [])
    selection = QueryTree("selection", "age > 30", [table], None)
    outer = QueryTree("projection", "a", [selection], None)

    assert rule.is_applicable(outer)
    assert rule.apply(outer) is None


def test_not_applicable_when_not_projection(rule):
    table = QueryTree("table", "employees", [])
    assert not rule.is_applicable(table)
