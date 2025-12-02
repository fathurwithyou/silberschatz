import pytest
from src.optimizer.rules.selection.commutativity import SelectionCommutativityRule
from src.core.models.query import QueryTree, QueryNodeType

@pytest.fixture
def rule():
    return SelectionCommutativityRule()


def test_swap_when_child_more_selective(rule):
    # Kasus: outer = "A != 3" (less selective), inner = "id = 5" (more selective).
    table = QueryTree(type=QueryNodeType.TABLE, value="T", children=[], parent=None)
    inner = QueryTree(type=QueryNodeType.SELECTION, value="id = 5", children=[table], parent=None)
    outer = QueryTree(type=QueryNodeType.SELECTION, value="A != 3", children=[inner], parent=None)
    table.parent = inner
    inner.parent = outer

    assert rule.is_applicable(outer)

    new_tree = rule.apply(outer)

    assert new_tree is not None
    assert new_tree.type == QueryNodeType.SELECTION
    assert new_tree.value == "id = 5"

    inner_after = new_tree.children[0]
    assert inner_after.type == QueryNodeType.SELECTION
    assert inner_after.value == "A != 3"

    assert inner_after.children[0].type == QueryNodeType.TABLE
    assert inner_after.children[0].value == "T"

    # parent pointers should be set correctly
    assert new_tree.children[0].parent is new_tree
    assert inner_after.children[0].parent is inner_after


def test_no_swap_when_parent_more_or_equal_selective(rule):
    # Kasus: outer = "id = 5" (very selective), inner = "A != 3" (less selective).
    table = QueryTree(type=QueryNodeType.TABLE, value="T", children=[], parent=None)
    inner = QueryTree(type=QueryNodeType.SELECTION, value="A != 3", children=[table], parent=None)
    outer = QueryTree(type=QueryNodeType.SELECTION, value="id = 5", children=[inner], parent=None)
    table.parent = inner
    inner.parent = outer

    assert rule.is_applicable(outer)

    new_tree = rule.apply(outer)

    assert new_tree is None


def test_not_applicable_when_no_nested_selection(rule):
    # Kalau node bukan selection atau child bukan selection, then is_applicable False
    table = QueryTree(type=QueryNodeType.TABLE, value="T", children=[], parent=None)
    selection_not_nested = QueryTree(type=QueryNodeType.SELECTION, value="id = 5", children=[table], parent=None)
    assert not rule.is_applicable(table)                # node is TABLE
    assert not rule.is_applicable(selection_not_nested) # child is TABLE, not SELECTION
