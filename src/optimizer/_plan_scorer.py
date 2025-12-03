import math
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from src.core.models.query import QueryTree, QueryNodeType
from src.core.models.storage import Statistic
from src.core import IStorageManager
from ._selectivity_estimator import SelectivityEstimator


@dataclass
class PlanScore:
    plan: QueryTree
    total_score: float
    selectivity_score: float
    join_order_score: float
    intermediate_size_score: float
    complexity_score: float


class PlanScorer:

    def __init__(self,
                 storage_manager: Optional[IStorageManager] = None,
                 heuristic_weights: Optional[Dict[str, float]] = None):
        self._storage_manager = storage_manager
        self._selectivity_estimator = SelectivityEstimator(storage_manager)

        default_weights = {
            'selectivity': 0.30,
            'join_order': 0.35,
            'intermediate_size': 0.25,
            'complexity': 0.10
        }
        self._weights = heuristic_weights or default_weights

    def score_plan(self, plan: QueryTree) -> PlanScore:
        selectivity_score = self._compute_selectivity_score(plan)
        join_order_score = self._compute_join_order_score(plan)
        intermediate_size_score = self._compute_intermediate_size_score(plan)
        complexity_score = self._compute_complexity_score(plan)

        total_score = (
            self._weights['selectivity'] * selectivity_score +
            self._weights['join_order'] * join_order_score +
            self._weights['intermediate_size'] * intermediate_size_score +
            self._weights['complexity'] * complexity_score
        )

        return PlanScore(
            plan=plan,
            total_score=total_score,
            selectivity_score=selectivity_score,
            join_order_score=join_order_score,
            intermediate_size_score=intermediate_size_score,
            complexity_score=complexity_score
        )

    def _compute_selectivity_score(self, plan: QueryTree) -> float:
        selections = []

        def find_selections(node, depth=0):
            if node.type == QueryNodeType.SELECTION:
                selections.append((node, depth))
            for child in node.children:
                find_selections(child, depth + 1)

        find_selections(plan)

        if not selections:
            return 0.0

        avg_depth = sum(depth for _, depth in selections) / len(selections)
        max_depth = self._get_max_depth(plan)

        return avg_depth / max(max_depth, 1)

    def _compute_join_order_score(self, plan: QueryTree) -> float:
        join_pairs = self._extract_join_pairs(plan)

        if not join_pairs:
            return 0.0

        score = 0.0
        for left_table, right_table in join_pairs:
            left_card = self._get_table_cardinality(left_table)
            right_card = self._get_table_cardinality(right_table)

            join_size = left_card * right_card
            max_card = max(left_card, right_card)
            min_card = max(min(left_card, right_card), 1)
            size_ratio = max_card / min_card

            if join_size > 0:
                base_score = math.log10(join_size) / 10.0
                imbalance_penalty = math.log10(size_ratio) / 5.0
                score += base_score + imbalance_penalty

        return score / len(join_pairs)

    def _compute_intermediate_size_score(self, plan: QueryTree) -> float:
        max_size = self._estimate_max_intermediate_size(plan)

        if max_size <= 0:
            return 0.0

        return min(math.log10(max_size) / 10.0, 1.0)

    def _compute_complexity_score(self, plan: QueryTree) -> float:
        node_count = self._count_nodes(plan)
        max_depth = self._get_max_depth(plan)

        complexity = (node_count / 20.0) + (max_depth / 10.0)
        return min(complexity, 1.0)

    def _extract_join_pairs(self, tree: QueryTree) -> List[Tuple[str, str]]:
        pairs = []

        def traverse(node):
            if node.type in [QueryNodeType.JOIN, QueryNodeType.THETA_JOIN, QueryNodeType.NATURAL_JOIN]:
                if len(node.children) >= 2:
                    left_tables = self._get_tables_from_subtree(node.children[0])
                    right_tables = self._get_tables_from_subtree(node.children[1])

                    if left_tables and right_tables:
                        pairs.append((left_tables[0], right_tables[0]))

            for child in node.children:
                traverse(child)

        traverse(tree)
        return pairs

    def _get_tables_from_subtree(self, node: QueryTree) -> List[str]:
        tables = []

        def traverse(n):
            if n.type == QueryNodeType.TABLE:
                table_name = n.value.split()[0] if n.value else "unknown"
                tables.append(table_name)
            for child in n.children:
                traverse(child)

        traverse(node)
        return tables

    def _get_table_cardinality(self, table_name: str) -> int:
        if self._storage_manager:
            try:
                stats = self._storage_manager.get_stats(table_name)
                return stats.n_r
            except Exception:
                pass
        return 1000

    def _estimate_max_intermediate_size(self, tree: QueryTree) -> int:
        max_size = 0

        def traverse(node):
            nonlocal max_size
            size = self._estimate_node_output_size(node)
            max_size = max(max_size, size)
            for child in node.children:
                traverse(child)

        traverse(tree)
        return max_size

    def _estimate_node_output_size(self, node: QueryTree) -> int:
        if node.type == QueryNodeType.TABLE:
            table_name = node.value.split()[0] if node.value else "unknown"
            return self._get_table_cardinality(table_name)

        elif node.type == QueryNodeType.SELECTION:
            if node.children:
                input_size = self._estimate_node_output_size(node.children[0])
                selectivity = self._selectivity_estimator.estimate_selection_selectivity(node)
                return max(int(input_size * selectivity), 1)
            return 1000

        elif node.type in [QueryNodeType.JOIN, QueryNodeType.THETA_JOIN, QueryNodeType.NATURAL_JOIN]:
            if len(node.children) >= 2:
                left_size = self._estimate_node_output_size(node.children[0])
                right_size = self._estimate_node_output_size(node.children[1])
                return left_size * right_size
            return 1000

        else:
            if node.children:
                return self._estimate_node_output_size(node.children[0])
            return 1000

    def _count_nodes(self, tree: QueryTree) -> int:
        count = 1
        for child in tree.children:
            count += self._count_nodes(child)
        return count

    def _get_max_depth(self, tree: QueryTree) -> int:
        if not tree.children:
            return 0
        return 1 + max(self._get_max_depth(child) for child in tree.children)
