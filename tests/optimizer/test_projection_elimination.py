import unittest
from src.optimizer.rules.projection.elimination import ProjectionEliminationRule
from src.core.models.query import QueryTree


class TestProjectionEliminationRule(unittest.TestCase):

    def test_intersection_kept_single_projection(self):
        # π(a,b) ( π(b,c) (R) )  ->  π(b) (R)
        rule = ProjectionEliminationRule()

        table = QueryTree(type="table", value="employees", children=[], parent=None)
        inner = QueryTree(type="projection", value="b,c", children=[table], parent=None)
        outer = QueryTree(type="projection", value="a,b", children=[inner], parent=None)

        self.assertTrue(rule.is_applicable(outer))
        new_tree = rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(set(map(str.strip, new_tree.value.split(","))), {"b"})
        self.assertEqual(new_tree.children[0].type, "table")
        self.assertEqual(new_tree.children[0].value, "employees")

    def test_outer_redundant_when_intersection_equals_inner(self):
        # π(a,b,c) ( π(b,c) (R) )  ->  π(b,c) (R)   (outer redundant)
        rule = ProjectionEliminationRule()

        table = QueryTree("table", "employees", [])
        inner = QueryTree("projection", "b,c", [table], None)
        outer = QueryTree("projection", "a,b,c", [inner], None)

        new_tree = rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(set(new_tree.value.split(",")), {"b", "c"})
        self.assertIs(new_tree, inner)
        self.assertIs(new_tree.children[0], table)

    def test_inner_redundant_when_intersection_equals_outer(self):
        # π(b) ( π(b,c) (R) )  ->  π(b) (R)   (inner redundant)
        rule = ProjectionEliminationRule()

        table = QueryTree("table", "employees", [])
        inner = QueryTree("projection", "b,c", [table], None)
        outer = QueryTree("projection", "b", [inner], None)

        new_tree = rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(set(new_tree.value.split(",")), {"b"})
        self.assertIs(new_tree.children[0], table)

    def test_inner_wildcard_outer_specific(self):
        # π(a,b) ( π(*) (R) )  ->  π(a,b) (R)   (inner is redundant)
        rule = ProjectionEliminationRule()

        table = QueryTree("table", "employees", [])
        inner = QueryTree("projection", "*", [table], None)
        outer = QueryTree("projection", "a,b", [inner], None)

        new_tree = rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(set(new_tree.value.split(",")), {"a", "b"})
        self.assertIs(new_tree.children[0], table)

    def test_outer_wildcard_inner_specific(self):
        # π(*) ( π(a,b) (R) )  ->  π(a,b) (R)   (outer is redundant)
        rule = ProjectionEliminationRule()

        table = QueryTree("table", "employees", [])
        inner = QueryTree("projection", "a,b", [table], None)
        outer = QueryTree("projection", "*", [inner], None)

        new_tree = rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(set(new_tree.value.split(",")), {"a", "b"})
        self.assertIs(new_tree, inner)
        self.assertIs(new_tree.children[0], table)

    def test_both_wildcards_collapse_to_single(self):
        # π(*) ( π(*) (R) )  ->  π(*) (R)
        rule = ProjectionEliminationRule()

        table = QueryTree("table", "employees", [])
        inner = QueryTree("projection", "*", [table], None)
        outer = QueryTree("projection", "*", [inner], None)

        new_tree = rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(new_tree.value, "*")
        self.assertIs(new_tree.children[0], table)

    def test_noop_when_child_not_projection(self):
        # π(a) ( σ(x > 1) (R) )  ->  no change (rule returns None)
        rule = ProjectionEliminationRule()

        table = QueryTree("table", "employees", [])
        selection = QueryTree("selection", "age > 30", [table], None)
        outer = QueryTree("projection", "a", [selection], None)

        self.assertTrue(rule.is_applicable(outer))
        self.assertIsNone(rule.apply(outer))

    def test_not_applicable_when_not_projection(self):
        rule = ProjectionEliminationRule()
        table = QueryTree("table", "employees", [])
        self.assertFalse(rule.is_applicable(table))


if __name__ == '__main__':
    unittest.main()
