import unittest
from src.core.models.query import QueryTree
from src.optimizer.rules.join.associativity import JoinAssociativityRule


class TestJoinAssociativityRule(unittest.TestCase):
    """Test cases for join associativity rule."""

    def setUp(self):
        """Set up test fixtures."""
        self.rule_right_deep = JoinAssociativityRule(prefer_right_deep=True)
        self.rule_left_deep = JoinAssociativityRule(prefer_right_deep=False)

    def test_is_applicable_left_deep_to_right_deep(self):
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

        self.assertTrue(self.rule_right_deep.is_applicable(outer_join))
        self.assertFalse(self.rule_left_deep.is_applicable(outer_join))

    def test_is_applicable_right_deep_to_left_deep(self):
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

        self.assertFalse(self.rule_right_deep.is_applicable(outer_join))
        self.assertTrue(self.rule_left_deep.is_applicable(outer_join))

    def test_is_not_applicable_non_join(self):
        """Test that rule is not applicable to non-join nodes."""
        table = QueryTree(type='table', value='T1', children=[])
        self.assertFalse(self.rule_right_deep.is_applicable(table))

    def test_is_not_applicable_no_nested_joins(self):
        """Test that rule is not applicable when there are no nested joins."""
        left = QueryTree(type='table', value='T1', children=[])
        right = QueryTree(type='table', value='T2', children=[])
        join = QueryTree(type='join', value='cond', children=[left, right])
        left.parent = join
        right.parent = join

        self.assertFalse(self.rule_right_deep.is_applicable(join))
        self.assertFalse(self.rule_left_deep.is_applicable(join))

    def test_apply_left_deep_to_right_deep(self):
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

        result = self.rule_right_deep.apply(outer_join)

        self.assertIsNotNone(result)
        self.assertEqual(result.type, 'join')
        # Root should have E1 on left and a join on right
        self.assertEqual(result.children[0].value, 'E1')
        self.assertEqual(result.children[1].type, 'join')
        # Right child join should have E2 and E3
        right_join = result.children[1]
        self.assertEqual(right_join.children[0].value, 'E2')
        self.assertEqual(right_join.children[1].value, 'E3')

    def test_apply_right_deep_to_left_deep(self):
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

        result = self.rule_left_deep.apply(outer_join)

        self.assertIsNotNone(result)
        self.assertEqual(result.type, 'join')
        # Root should have a join on left and E3 on right
        self.assertEqual(result.children[0].type, 'join')
        self.assertEqual(result.children[1].value, 'E3')
        # Left child join should have E1 and E2
        left_join = result.children[0]
        self.assertEqual(left_join.children[0].value, 'E1')
        self.assertEqual(left_join.children[1].value, 'E2')

    def test_combine_conditions(self):
        """Test condition combination logic."""
        result = self.rule_right_deep._combine_conditions('cond1', 'cond2')
        self.assertEqual(result, '(cond1) AND (cond2)')

        result = self.rule_right_deep._combine_conditions('', 'cond2')
        self.assertEqual(result, 'cond2')

        result = self.rule_right_deep._combine_conditions('cond1', '')
        self.assertEqual(result, 'cond1')

        result = self.rule_right_deep._combine_conditions('', '')
        self.assertEqual(result, '')

    def test_apply_returns_none_when_not_applicable(self):
        """Test that apply returns None for non-applicable nodes."""
        table = QueryTree(type='table', value='T1', children=[])
        result = self.rule_right_deep.apply(table)
        self.assertIsNone(result)

    def test_name_property(self):
        """Test that rule has correct name."""
        self.assertEqual(self.rule_right_deep.name, "JoinAssociativity")


if __name__ == '__main__':
    unittest.main()
