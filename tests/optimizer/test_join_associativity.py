import pytest
from src.core.models.query import QueryTree
from src.optimizer.rules.join.associativity import JoinAssociativityRule


@pytest.fixture
def rule_right_deep():
    return JoinAssociativityRule(prefer_right_deep=True)


@pytest.fixture
def rule_left_deep():
    return JoinAssociativityRule(prefer_right_deep=False)


def test_is_applicable_left_deep_to_right_deep(rule_right_deep, rule_left_deep):
    """Test applicability for left-deep pattern (E1 ⋈ E2) ⋈ E3."""
    e1 = QueryTree(type='table', value='E1', children=[])
    e2 = QueryTree(type='table', value='E2', children=[])
    e3 = QueryTree(type='table', value='E3', children=[])

    inner_join = QueryTree(type='join', value='cond1', children=[e1, e2])
    e1.parent = inner_join
    e2.parent = inner_join

    outer_join = QueryTree(type='join', value='cond2', children=[inner_join, e3])
    inner_join.parent = outer_join
    e3.parent = outer_join

    assert rule_right_deep.is_applicable(outer_join)
    assert not rule_left_deep.is_applicable(outer_join)


def test_is_applicable_right_deep_to_left_deep(rule_right_deep, rule_left_deep):
    """Test applicability for right-deep pattern E1 ⋈ (E2 ⋈ E3)."""
    e1 = QueryTree(type='table', value='E1', children=[])
    e2 = QueryTree(type='table', value='E2', children=[])
    e3 = QueryTree(type='table', value='E3', children=[])

    inner_join = QueryTree(type='join', value='cond2', children=[e2, e3])
    e2.parent = inner_join
    e3.parent = inner_join

    outer_join = QueryTree(type='join', value='cond1', children=[e1, inner_join])
    e1.parent = outer_join
    inner_join.parent = outer_join

    assert not rule_right_deep.is_applicable(outer_join)
    assert rule_left_deep.is_applicable(outer_join)


def test_is_not_applicable_non_join(rule_right_deep):
    """Test that rule is not applicable to non-join nodes."""
    table = QueryTree(type='table', value='T1', children=[])
    assert not rule_right_deep.is_applicable(table)


def test_is_not_applicable_no_nested_joins(rule_right_deep, rule_left_deep):
    """Test that rule is not applicable when there are no nested joins."""
    left = QueryTree(type='table', value='T1', children=[])
    right = QueryTree(type='table', value='T2', children=[])
    join = QueryTree(type='join', value='cond', children=[left, right])
    left.parent = join
    right.parent = join

    assert not rule_right_deep.is_applicable(join)
    assert not rule_left_deep.is_applicable(join)


def test_apply_left_deep_to_right_deep(rule_right_deep):
    """Test conversion from (E1 ⋈ E2) ⋈ E3 to E1 ⋈ (E2 ⋈ E3)."""
    e1 = QueryTree(type='table', value='E1', children=[])
    e2 = QueryTree(type='table', value='E2', children=[])
    e3 = QueryTree(type='table', value='E3', children=[])

    inner_join = QueryTree(type='join', value='cond1', children=[e1, e2])
    e1.parent = inner_join
    e2.parent = inner_join

    outer_join = QueryTree(type='join', value='cond2', children=[inner_join, e3])
    inner_join.parent = outer_join
    e3.parent = outer_join

    result = rule_right_deep.apply(outer_join)

    assert result is not None
    assert result.type == 'join'
    # Root should have E1 on left and a join on right
    assert result.children[0].value == 'E1'
    assert result.children[1].type == 'join'
    # Right child join should have E2 and E3
    right_join = result.children[1]
    assert right_join.children[0].value == 'E2'
    assert right_join.children[1].value == 'E3'


def test_apply_right_deep_to_left_deep(rule_left_deep):
    """Test conversion from E1 ⋈ (E2 ⋈ E3) to (E1 ⋈ E2) ⋈ E3."""
    e1 = QueryTree(type='table', value='E1', children=[])
    e2 = QueryTree(type='table', value='E2', children=[])
    e3 = QueryTree(type='table', value='E3', children=[])

    inner_join = QueryTree(type='join', value='cond2', children=[e2, e3])
    e2.parent = inner_join
    e3.parent = inner_join

    outer_join = QueryTree(type='join', value='cond1', children=[e1, inner_join])
    e1.parent = outer_join
    inner_join.parent = outer_join

    result = rule_left_deep.apply(outer_join)

    assert result is not None
    assert result.type == 'join'
    # Root should have a join on left and E3 on right
    assert result.children[0].type == 'join'
    assert result.children[1].value == 'E3'
    # Left child join should have E1 and E2
    left_join = result.children[0]
    assert left_join.children[0].value == 'E1'
    assert left_join.children[1].value == 'E2'


def test_combine_conditions(rule_right_deep):
    """Test condition combination logic."""
    result = rule_right_deep._combine_conditions('cond1', 'cond2')
    assert result == '(cond1) AND (cond2)'

    result = rule_right_deep._combine_conditions('', 'cond2')
    assert result == 'cond2'

    result = rule_right_deep._combine_conditions('cond1', '')
    assert result == 'cond1'

    result = rule_right_deep._combine_conditions('', '')
    assert result == ''


def test_apply_returns_none_when_not_applicable(rule_right_deep):
    """Test that apply returns None for non-applicable nodes."""
    table = QueryTree(type='table', value='T1', children=[])
    result = rule_right_deep.apply(table)
    assert result is None


def test_name_property(rule_right_deep):
    """Test that rule has correct name."""
    assert rule_right_deep.name == "JoinAssociativity"
