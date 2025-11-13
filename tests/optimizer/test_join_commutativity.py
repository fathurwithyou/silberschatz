"""Unit tests for JoinCommutativityRule."""
import unittest
from src.core.models.query import QueryTree
from src.optimizer.rules.join.commutativity import JoinCommutativityRule


class TestJoinCommutativityRule(unittest.TestCase):
    """Test cases for join commutativity rule."""

    def setUp(self):
        """Set up test fixtures."""
        self.rule = JoinCommutativityRule()

    def test_is_applicable_with_valid_join(self):
        """Test that rule is applicable to a join with 2 children."""
        left = QueryTree(type='table', value='T1', children=[])
        right = QueryTree(type='table', value='T2', children=[])
        join = QueryTree(type='join', value='T1.id = T2.id', children=[left, right])
        left.parent = join
        right.parent = join

        self.assertTrue(self.rule.is_applicable(join))

    def test_is_not_applicable_with_non_join(self):
        """Test that rule is not applicable to non-join nodes."""
        selection = QueryTree(type='selection', value='age > 18', children=[])
        self.assertFalse(self.rule.is_applicable(selection))

    def test_is_not_applicable_with_wrong_children_count(self):
        """Test that rule is not applicable to join with != 2 children."""
        child1 = QueryTree(type='table', value='T1', children=[])
        child2 = QueryTree(type='table', value='T2', children=[])
        child3 = QueryTree(type='table', value='T3', children=[])
        join = QueryTree(type='join', value='cond', children=[child1, child2, child3])

        self.assertFalse(self.rule.is_applicable(join))

    def test_apply_swaps_children(self):
        """Test that apply correctly swaps left and right children."""
        left = QueryTree(type='table', value='Employee', children=[])
        right = QueryTree(type='table', value='Department', children=[])
        join = QueryTree(type='join', value='E.dept_id = D.id', children=[left, right])
        left.parent = join
        right.parent = join

        result = self.rule.apply(join)

        self.assertIsNotNone(result)
        self.assertEqual(result.type, 'join')
        self.assertEqual(result.value, 'E.dept_id = D.id')
        self.assertEqual(result.children[0].value, 'Department')
        self.assertEqual(result.children[1].value, 'Employee')

    def test_apply_updates_parent_references(self):
        """Test that parent references are correctly updated after swap."""
        left = QueryTree(type='table', value='T1', children=[])
        right = QueryTree(type='table', value='T2', children=[])
        join = QueryTree(type='join', value='cond', children=[left, right])
        left.parent = join
        right.parent = join

        result = self.rule.apply(join)

        self.assertEqual(result.children[0].parent, result)
        self.assertEqual(result.children[1].parent, result)

    def test_apply_with_custom_swap_function(self):
        """Test that custom swap decision function is respected."""
        # Custom function that never swaps
        rule = JoinCommutativityRule(should_swap_fn=lambda l, r: False)

        left = QueryTree(type='table', value='T1', children=[])
        right = QueryTree(type='table', value='T2', children=[])
        join = QueryTree(type='join', value='cond', children=[left, right])
        left.parent = join
        right.parent = join

        result = rule.apply(join)

        self.assertIsNone(result)

    def test_apply_returns_none_when_not_applicable(self):
        """Test that apply returns None for non-applicable nodes."""
        selection = QueryTree(type='selection', value='age > 18', children=[])
        result = self.rule.apply(selection)
        self.assertIsNone(result)

    def test_name_property(self):
        """Test that rule has correct name."""
        self.assertEqual(self.rule.name, "JoinCommutativity")


if __name__ == '__main__':
    unittest.main()
