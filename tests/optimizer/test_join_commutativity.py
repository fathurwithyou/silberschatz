"""Unit tests for JoinCommutativityRule."""
import pytest
from src.core.models.query import QueryTree
from src.optimizer.rules.join.commutativity import JoinCommutativityRule


@pytest.fixture
def rule():
    return JoinCommutativityRule()


def test_is_applicable_with_valid_join(rule):
    """Test that rule is applicable to a join with 2 children."""
    left = QueryTree(type='table', value='T1', children=[])
    right = QueryTree(type='table', value='T2', children=[])
    join = QueryTree(type='join', value='T1.id = T2.id', children=[left, right])
    left.parent = join
    right.parent = join

    assert rule.is_applicable(join)


def test_is_not_applicable_with_non_join(rule):
    """Test that rule is not applicable to non-join nodes."""
    selection = QueryTree(type='selection', value='age > 18', children=[])
    assert not rule.is_applicable(selection)


def test_is_not_applicable_with_wrong_children_count(rule):
    """Test that rule is not applicable to join with != 2 children."""
    child1 = QueryTree(type='table', value='T1', children=[])
    child2 = QueryTree(type='table', value='T2', children=[])
    child3 = QueryTree(type='table', value='T3', children=[])
    join = QueryTree(type='join', value='cond', children=[child1, child2, child3])

    assert not rule.is_applicable(join)


def test_apply_swaps_children(rule):
    """Test that apply correctly swaps left and right children."""
    left = QueryTree(type='table', value='Employee', children=[])
    right = QueryTree(type='table', value='Department', children=[])
    join = QueryTree(type='join', value='E.dept_id = D.id', children=[left, right])
    left.parent = join
    right.parent = join

    result = rule.apply(join)

    assert result is not None
    assert result.type == 'join'
    assert result.value == 'E.dept_id = D.id'
    assert result.children[0].value == 'Department'
    assert result.children[1].value == 'Employee'


def test_apply_updates_parent_references(rule):
    """Test that parent references are correctly updated after swap."""
    left = QueryTree(type='table', value='T1', children=[])
    right = QueryTree(type='table', value='T2', children=[])
    join = QueryTree(type='join', value='cond', children=[left, right])
    left.parent = join
    right.parent = join

    result = rule.apply(join)

    assert result.children[0].parent == result
    assert result.children[1].parent == result


def test_apply_with_custom_swap_function():
    """Test that custom swap decision function is respected."""
    custom_rule = JoinCommutativityRule(should_swap_fn=lambda l, r: False)

    left = QueryTree(type='table', value='T1', children=[])
    right = QueryTree(type='table', value='T2', children=[])
    join = QueryTree(type='join', value='cond', children=[left, right])
    left.parent = join
    right.parent = join

    result = custom_rule.apply(join)

    assert result is None


def test_apply_returns_none_when_not_applicable(rule):
    """Test that apply returns None for non-applicable nodes."""
    selection = QueryTree(type='selection', value='age > 18', children=[])
    result = rule.apply(selection)
    assert result is None


def test_name_property(rule):
    """Test that rule has correct name."""
    assert rule.name == "JoinCommutativity"
