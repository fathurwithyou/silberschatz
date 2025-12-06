from typing import List, Optional, Dict
from src.core.models import ParsedQuery
from src.core.models.query import QueryTree
from src.core import IQueryOptimizer, IStorageManager
from .parser import QueryParser
from .cost.cost_model import CostModel
from .rules import (
    JoinCommutativityRule,
    JoinAssociativityRule,
    ProjectionEliminationRule,
    SelectionCartesianProductRule,
    SelectionCommutativityRule,
    SelectionDecompositionRule,
    SelectionJoinDistributionRule,
    SelectionThetaJoinRule,
)
from ._plan_scorer import PlanScorer
from ._candidate_generator import CandidateGenerator
from ._plan_utils import count_joins


class QueryOptimizer(IQueryOptimizer):

    def __init__(self,
                 storage_manager: IStorageManager,
                 max_iterations: int = 10,
                 num_candidates: int = 5,
                 use_heuristics: bool = True,
                 heuristic_weights: Optional[Dict[str, float]] = None):

        self._storage_manager = storage_manager
        self._max_iterations = max_iterations
        self._parser = QueryParser()
        self._num_candidates = num_candidates
        self._use_heuristics = use_heuristics

        self._rules = [
            JoinCommutativityRule(),
            JoinAssociativityRule(prefer_right_deep=False),
            ProjectionEliminationRule(),
            SelectionCartesianProductRule(),
            SelectionCommutativityRule(),
            SelectionDecompositionRule(),
            SelectionThetaJoinRule(),
            SelectionJoinDistributionRule(self._storage_manager),
        ]

        self._cost_model = CostModel(storage_manager=self._storage_manager)

        self._plan_scorer = PlanScorer(
            storage_manager=self._storage_manager,
            heuristic_weights=heuristic_weights
        )

        self._candidate_generator = CandidateGenerator(
            storage_manager=self._storage_manager
        )

    @property
    def rules(self) -> List:
        return self._rules

    def parse_query(self, query: str) -> ParsedQuery:
        return self._parser(query)

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        transformed = self._apply_basic_transformations(query.tree)

        if not self._use_heuristics:
            return ParsedQuery(tree=transformed, query=query.query)

        if not self._needs_candidate_generation(transformed):
            return ParsedQuery(tree=transformed, query=query.query)

        candidates = self._candidate_generator.generate_candidates(
            transformed,
            self._num_candidates
        )

        scored_plans = [self._plan_scorer.score_plan(candidate) for candidate in candidates]

        if scored_plans:
            best_plan = min(scored_plans, key=lambda x: x.total_score)
            return ParsedQuery(tree=best_plan.plan, query=query.query)
        else:
            return ParsedQuery(tree=transformed, query=query.query)

    def get_cost(self, query: ParsedQuery) -> float:
        if self._cost_model is None:
            raise NotImplementedError("Cost model not initialized.")
        return self._cost_model.get_cost(query.tree)

    def _apply_basic_transformations(self, tree: QueryTree) -> QueryTree:
        beneficial_rules = [
            SelectionCartesianProductRule(), 
            SelectionDecompositionRule(), 
            ProjectionEliminationRule(),
        ]

        current = tree
        changed = True
        iterations = 0

        while changed and iterations < self._max_iterations:
            changed = False
            iterations += 1

            for rule in beneficial_rules:
                new_tree = self._apply_rule_bottom_up(current, rule)
                if new_tree and new_tree is not current:
                    current = new_tree
                    changed = True
                    break

        return current

    def _apply_rule_bottom_up(self, tree: QueryTree, rule) -> Optional[QueryTree]:
        from copy import deepcopy

        modified = False

        def traverse(node):
            nonlocal modified

            for i, child in enumerate(node.children):
                result = traverse(child)
                if result:
                    node.children[i] = result
                    result.parent = node
                    modified = True

            if rule.is_applicable(node):
                optimized = rule.apply(node)
                if optimized:
                    modified = True
                    return optimized

            return None

        new_tree = deepcopy(tree)
        result = traverse(new_tree)

        if result:
            # Root node was transformed
            return result
        elif modified:
            # Some child nodes were transformed
            return new_tree
        else:
            # No transformations occurred anywhere in the tree
            # Return the original tree to indicate no change
            return tree

    def _needs_candidate_generation(self, tree: QueryTree) -> bool:
        num_joins = count_joins(tree)
        return num_joins >= 2
