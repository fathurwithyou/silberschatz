import unittest
from src.optimizer.rules.projection.pushdown import ProjectionPushdownRule
from src.core.models.query import QueryTree


class TestProjectionPushdownRule(unittest.TestCase):

    def test_pushdown_through_selection(self):
        # π ( name, age ) on σ(age > 30)(employees)
        rule = ProjectionPushdownRule()

        table = QueryTree(type="table", value="employees", children=[], parent=None)

        selection = QueryTree(
            type="selection",
            value="age > 30",
            children=[table],
            parent=None
        )

        proj = QueryTree(
            type="projection",
            value="name, age",
            children=[selection],
            parent=None
        )

        self.assertTrue(rule.is_applicable(proj))
        new_tree = rule.apply(proj)

        self.assertEqual(new_tree.type, "projection")
        self.assertEqual(new_tree.value, "name, age")

        child_sel = new_tree.children[0]
        self.assertEqual(child_sel.type, "selection")

        pushed_proj = child_sel.children[0]
        self.assertEqual(pushed_proj.type, "projection")
        self.assertIn("age", pushed_proj.value)
        self.assertIn("name", pushed_proj.value)

        leaf = pushed_proj.children[0]
        self.assertEqual(leaf.type, "table")
        self.assertEqual(leaf.value, "employees")


    def test_pushdown_through_order_by(self):
        # π(name) on ORDER BY age (employees)
        rule = ProjectionPushdownRule()

        table = QueryTree("table", "employees", [])
        order_node = QueryTree("order_by", "age DESC", [table])
        proj = QueryTree("projection", "name", [order_node])

        new_tree = rule.apply(proj)

        self.assertEqual(new_tree.type, "projection")

        child_order = new_tree.children[0]
        self.assertEqual(child_order.type, "order_by")

        pushed_proj = child_order.children[0]
        self.assertEqual(pushed_proj.type, "projection")

        self.assertIn("name", pushed_proj.value)
        self.assertIn("age", pushed_proj.value)


    def test_pushdown_through_limit(self):
        # π(name) on LIMIT 10 (employees)
        rule = ProjectionPushdownRule()

        table = QueryTree("table", "employees", [])
        limit = QueryTree("limit", "10", [table])
        proj = QueryTree("projection", "name", [limit])

        new_tree = rule.apply(proj)

        # top still projection
        self.assertEqual(new_tree.type, "projection")

        child_limit = new_tree.children[0]
        self.assertEqual(child_limit.type, "limit")

        pushed_proj = child_limit.children[0]
        self.assertEqual(pushed_proj.type, "projection")
        self.assertEqual(pushed_proj.value, "name")

        self.assertEqual(pushed_proj.children[0].value, "employees")


    def test_pushdown_through_join_split(self):
        # SELECT name FROM employees JOIN departments ON employees.dept_id = departments.id
        rule = ProjectionPushdownRule()

        left = QueryTree("table", "employees", [])
        right = QueryTree("table", "departments", [])

        join = QueryTree(
            type="join",
            value="employees.dept_id = departments.id",
            children=[left, right],
            parent=None
        )

        proj = QueryTree(
            type="projection",
            value="employees.name",
            children=[join],
            parent=None
        )

        self.assertTrue(rule.is_applicable(proj))
        new_tree = rule.apply(proj)

        self.assertEqual(new_tree.type, "projection")

        updated_join = new_tree.children[0]
        self.assertEqual(updated_join.type, "join")

        new_left = updated_join.children[0]
        self.assertEqual(new_left.type, "projection")
        self.assertIn("employees.name", new_left.value)
        self.assertIn("employees.dept_id", new_left.value)

        new_right = updated_join.children[1]
        self.assertEqual(new_right.type, "projection")
        self.assertIn("departments.id", new_right.value)

        self.assertEqual(new_left.children[0].value, "employees")
        self.assertEqual(new_right.children[0].value, "departments")


if __name__ == '__main__':
    unittest.main()
