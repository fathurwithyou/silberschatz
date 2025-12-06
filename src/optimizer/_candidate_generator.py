from typing import List, Optional, Dict
from src.core.models.query import QueryTree, QueryNodeType
from src.core.models.storage import Statistic
from src.optimizer.rules import (
    SelectionCartesianProductRule,
    SelectionJoinDistributionRule,
    JoinCommutativityRule,
    JoinAssociativityRule
)
from src.core import IStorageManager


class CandidateGenerator:

    def __init__(self,
                 storage_manager: IStorageManager):
        self._storage_manager = storage_manager

    def generate_candidates(self, tree: QueryTree, num_candidates: int = 5) -> List[QueryTree]:
        candidates = []

        candidates.append(tree)

        selection_pushed = self._apply_selection_pushdown(tree)
        if selection_pushed and not self._is_same_plan(selection_pushed, tree):
            candidates.append(selection_pushed)

        small_first = self._generate_small_tables_first_plan(tree)
        if small_first and not any(self._is_same_plan(small_first, c) for c in candidates):
            candidates.append(small_first)

        selective_first = self._generate_selective_filters_first_plan(tree)
        if selective_first and not any(self._is_same_plan(selective_first, c) for c in candidates):
            candidates.append(selective_first)

        bushy = self._generate_bushy_tree_plan(tree)
        if bushy and not any(self._is_same_plan(bushy, c) for c in candidates):
            candidates.append(bushy)

        return candidates[:num_candidates]

    def _apply_selection_pushdown(self, tree: QueryTree) -> Optional[QueryTree]:
        from copy import deepcopy
        plan = deepcopy(tree)

        rule = SelectionJoinDistributionRule(self._storage_manager)
        changed = True
        iterations = 0
        max_iterations = 5

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            new_plan = self._apply_rule_bottom_up(plan, rule)
            if new_plan and not self._is_same_plan(new_plan, plan):
                plan = new_plan
                changed = True

        return plan

    def _generate_small_tables_first_plan(self, tree: QueryTree) -> Optional[QueryTree]:
        # Skip this optimization if there are JOIN conditions
        # Reordering joins with conditions requires complex analysis
        join_conditions = self._extract_join_conditions(tree)
        if join_conditions:
            return None

        relations = self._extract_relations(tree)
        if len(relations) < 2:
            return None

        sorted_relations = sorted(relations, key=lambda r: self._get_table_cardinality(r))
        return self._build_left_deep_tree(sorted_relations, [])

    def _generate_selective_filters_first_plan(self, tree: QueryTree) -> Optional[QueryTree]:
        return self._apply_selection_pushdown(tree)

    def _generate_bushy_tree_plan(self, tree: QueryTree) -> Optional[QueryTree]:
        # Skip bushy tree optimization if there are JOIN conditions
        # Reordering joins with associativity requires validating that conditions
        # remain applicable after transformation
        join_conditions = self._extract_join_conditions(tree)
        if join_conditions:
            return None

        from copy import deepcopy
        plan = deepcopy(tree)

        rule = JoinAssociativityRule(prefer_right_deep=True)
        changed = True
        iterations = 0
        max_iterations = 3

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            new_plan = self._apply_rule_bottom_up(plan, rule)
            if new_plan and not self._is_same_plan(new_plan, plan):
                plan = new_plan
                changed = True

        return plan

    def _apply_rule_bottom_up(self, tree: QueryTree, rule) -> Optional[QueryTree]:
        from copy import deepcopy

        def traverse(node):
            for i, child in enumerate(node.children):
                result = traverse(child)
                if result:
                    node.children[i] = result
                    result.parent = node

            if rule.is_applicable(node):
                optimized = rule.apply(node)
                if optimized:
                    return optimized

            return None

        new_tree = deepcopy(tree)
        traverse(new_tree)
        return new_tree

    def _extract_relations(self, tree: QueryTree) -> List[str]:
        relations = []

        def traverse(node):
            if node.type == QueryNodeType.TABLE:
                table_name = node.value.split()[0] if node.value else ""
                if table_name and table_name not in relations:
                    relations.append(table_name)
            for child in node.children:
                traverse(child)

        traverse(tree)
        return relations

    def _extract_join_conditions(self, tree: QueryTree) -> List[str]:
        """Extract all JOIN conditions from the query tree."""
        conditions = []

        def traverse(node):
            if node.type == QueryNodeType.JOIN and node.value:
                conditions.append(node.value)
            for child in node.children:
                traverse(child)

        traverse(tree)
        return conditions

    def _get_table_cardinality(self, table_name: str) -> int:
        try:
            stats = self._storage_manager.get_stats(table_name)
            return stats.n_r
        except Exception:
            return 1000

    def _build_left_deep_tree(self, relations: List[str], join_conditions: List[str] = None) -> Optional[QueryTree]:
        if not relations:
            return None

        if join_conditions is None:
            join_conditions = []

        current = QueryTree(
            type=QueryNodeType.TABLE,
            value=relations[0],
            children=[]
        )

        for i in range(1, len(relations)):
            right = QueryTree(
                type=QueryNodeType.TABLE,
                value=relations[i],
                children=[]
            )

            # Find matching condition for this join
            condition = ""
            if join_conditions:
                # Try to find a condition that relates current tables
                for cond in join_conditions:
                    # Simple heuristic: use first available condition
                    # A more sophisticated approach would match tables in condition
                    if cond:
                        condition = cond
                        break

            current = QueryTree(
                type=QueryNodeType.JOIN,
                value=condition,
                children=[current, right]
            )

        return current

    def _is_same_plan(self, plan1: QueryTree, plan2: QueryTree) -> bool:
        if plan1.type != plan2.type:
            return False
        if len(plan1.children) != len(plan2.children):
            return False
        return all(
            self._is_same_plan(c1, c2)
            for c1, c2 in zip(plan1.children, plan2.children)
        )
