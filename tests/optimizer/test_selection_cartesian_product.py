import pytest
from src.optimizer.rules.selection.cartesian_product import SelectionCartesianProductRule
from src.core.models.query import QueryTree, QueryNodeType


@pytest.fixture
def rule():
    return SelectionCartesianProductRule()


def test_convert_cartesian_to_theta_join(rule):
    # σ(A.x = B.y)(A × B)  to  A ⋈(A.x = B.y) B
    left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[], parent=None)
    right = QueryTree(type=QueryNodeType.TABLE, value="B", children=[], parent=None)

    cart = QueryTree(type=QueryNodeType.CARTESIAN_PRODUCT, value=None, children=[left, right], parent=None)
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.x = B.y", children=[cart], parent=None)

    # set parent links as in a real tree
    left.parent = cart
    right.parent = cart
    cart.parent = sel

    assert rule.is_applicable(sel)
    new_tree = rule.apply(sel)

    assert new_tree is not None
    assert new_tree.type == QueryNodeType.THETA_JOIN
    assert new_tree.value == "A.x = B.y"
    assert len(new_tree.children) == 2
    assert new_tree.children[0].type == QueryNodeType.TABLE
    assert new_tree.children[0].value == "A"
    assert new_tree.children[1].type == QueryNodeType.TABLE
    assert new_tree.children[1].value == "B"

    # parent pointers updated
    assert new_tree.children[0].parent is new_tree
    assert new_tree.children[1].parent is new_tree


def test_not_applicable_when_not_cartesian(rule):
    left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[], parent=None)
    join = QueryTree(type=QueryNodeType.JOIN, value=None, children=[left], parent=None)
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.x = 1", children=[join], parent=None)

    assert not rule.is_applicable(sel)
    assert rule.apply(sel) is None


def test_not_applicable_when_cartesian_has_less_than_two_children(rule):
    only_one = QueryTree(type=QueryNodeType.TABLE, value="A", children=[], parent=None)
    cart = QueryTree(type=QueryNodeType.CARTESIAN_PRODUCT, value=None, children=[only_one], parent=None)
    sel = QueryTree(type=QueryNodeType.SELECTION, value="A.x = 1", children=[cart], parent=None)

    assert rule.is_applicable(sel)
    assert rule.apply(sel) is None
