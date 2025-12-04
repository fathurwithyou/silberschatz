from src.core.models.query import QueryTree, QueryNodeType


def count_joins(tree: QueryTree) -> int:
    count = 0

    def traverse(node):
        nonlocal count
        if node.type in [QueryNodeType.JOIN, QueryNodeType.THETA_JOIN, QueryNodeType.NATURAL_JOIN]:
            count += 1
        for child in node.children:
            traverse(child)

    traverse(tree)
    return count


def count_nodes(tree: QueryTree) -> int:
    count = 1
    for child in tree.children:
        count += count_nodes(child)
    return count


def get_max_depth(tree: QueryTree) -> int:
    if not tree.children:
        return 0
    return 1 + max(get_max_depth(child) for child in tree.children)


def is_same_plan(plan1: QueryTree, plan2: QueryTree) -> bool:
    if plan1.type != plan2.type:
        return False
    if len(plan1.children) != len(plan2.children):
        return False
    return all(
        is_same_plan(c1, c2)
        for c1, c2 in zip(plan1.children, plan2.children)
    )
