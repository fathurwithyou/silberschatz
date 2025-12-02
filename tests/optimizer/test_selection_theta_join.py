import pytest
from src.optimizer.rules.selection.theta_join import SelectionThetaJoinRule
from src.core.models.query import QueryTree, QueryNodeType


@pytest.fixture
def rule():
    return SelectionThetaJoinRule()


def test_is_applicable_valid(rule):
    join = QueryTree(type=QueryNodeType.THETA_JOIN, value="A.id = B.id", children=[])
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[join])

    assert rule.is_applicable(sel)


def test_is_applicable_invalid_type(rule):
    node = QueryTree(type=QueryNodeType.JOIN, value=None, children=[])
    assert not rule.is_applicable(node)


def test_is_applicable_invalid_child(rule):
    child = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
    node = QueryTree(type=QueryNodeType.SELECTION, value="x > 0", children=[child])
    assert not rule.is_applicable(node)


def test_apply_merge_conditions(rule):
    left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
    right = QueryTree(type=QueryNodeType.TABLE, value="B", children=[])
    join = QueryTree(type=QueryNodeType.THETA_JOIN, value="A.id = B.id", children=[left, right])
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[join])

    new_node = rule.apply(sel)

    assert new_node is not None
    assert new_node.type == QueryNodeType.THETA_JOIN
    assert new_node.value == "A.id = B.id AND A.age > 30"
    assert len(new_node.children) == 2


def test_apply_when_only_selection_condition(rule):
    # Jika join tidak punya kondisi, pakai selection condition
    left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
    right = QueryTree(type=QueryNodeType.TABLE, value="B", children=[])
    join = QueryTree(type=QueryNodeType.THETA_JOIN, value="", children=[left, right])
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[join])

    new_node = rule.apply(sel)

    assert new_node is not None
    assert new_node.type == QueryNodeType.THETA_JOIN
    assert new_node.value == "A.age > 30"


def test_apply_not_applicable(rule):
    table = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[table])

    assert rule.apply(sel) is None
