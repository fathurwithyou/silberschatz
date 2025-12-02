import pytest
from src.optimizer.rules.selection.decomposition import SelectionDecompositionRule
from src.core.models.query import QueryTree, QueryNodeType


@pytest.fixture
def rule():
    return SelectionDecompositionRule()


def test_decompose_simple_and(rule):
    # QueryTree: selection with AND
    child = QueryTree(type=QueryNodeType.TABLE, value="employees", children=[], parent=None)
    node = QueryTree(type=QueryNodeType.SELECTION, value="age > 30 AND salary < 5000", children=[child], parent=None)

    assert rule.is_applicable(node)

    new_tree = rule.apply(node)
    assert new_tree is not None

    # Should produce nested selections
    assert new_tree.type == QueryNodeType.SELECTION
    assert new_tree.value == "age > 30"
    assert new_tree.children[0].type == QueryNodeType.SELECTION
    assert new_tree.children[0].value == "salary < 5000"
