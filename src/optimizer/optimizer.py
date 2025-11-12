from typing import List, Optional, Dict
from src.core.models import ParsedQuery
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic
from src.core.optimizer import IQueryOptimizer
from .parser import QueryParser
from .cost.cost_model import CostModel
from .rules import (
    JoinCommutativityRule,
    JoinAssociativityRule,
    ProjectionEliminationRule,
    ProjectionPushdownRule,
    SelectionCartesianProductRule,
    SelectionCommutativityRule,
    SelectionDecompositionRule,
    SelectionJoinDistributionRule,
    SelectionThetaJoinRule,
)


class QueryOptimizer(IQueryOptimizer):

    def __init__(self,
                 rules: Optional[List] = None,
                 statistics: Optional[Dict[str, Statistic]] = None,
                 max_iterations: int = 10):

        # Use default rules if none provided
        if rules is None:
            rules = self._get_default_rules()

        self._rules = rules
        self._max_iterations = max_iterations
        self._parser = QueryParser()

        if statistics is not None:
            self._cost_model = CostModel(statistics)
        else:
            self._cost_model = None

    def _get_default_rules(self) -> List:

        return [
            JoinCommutativityRule(),
            JoinAssociativityRule(prefer_right_deep=True),
            ProjectionEliminationRule(),
            ProjectionPushdownRule(),
            SelectionCartesianProductRule(),
            SelectionCommutativityRule(),
            SelectionDecompositionRule(),
            SelectionThetaJoinRule(),
            # TODO: Implement storage manager
            # SelectionJoinDistributionRule(),
        ]

    def parse_query(self, query: str) -> ParsedQuery:
        return self._parser(query)

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        current_tree = query.tree
        iteration = 0

        while iteration < self._max_iterations:
            iteration += 1
            optimized = self._apply_rules_once(current_tree)

            # Stop if no changes were made
            if optimized is current_tree:
                break

            current_tree = optimized

        return ParsedQuery(tree=current_tree, query=query.query)

    def get_cost(self, query: ParsedQuery) -> float:
        if self._cost_model is None:
            raise NotImplementedError("Cost model not initialized. Provide statistics to constructor.")
        return self._cost_model.get_cost(query.tree)

    def _apply_rules_once(self, tree: QueryTree) -> QueryTree:
        optimized_children = []
        children_changed = False

        for child in tree.children:
            optimized_child = self._apply_rules_once(child)
            optimized_children.append(optimized_child)
            if optimized_child is not child:
                children_changed = True

        # Create new node if children changed
        if children_changed:
            tree = QueryTree(
                type=tree.type,
                value=tree.value,
                children=optimized_children,
                parent=tree.parent
            )
            for child in tree.children:
                child.parent = tree

        for rule in self._rules:
            if rule.is_applicable(tree):
                optimized = rule.apply(tree)
                if optimized is not None:
                    return optimized

        return tree

    def add_rule(self, rule):
        self._rules.append(rule)

    def remove_rule(self, rule_name: str):
        self._rules = [r for r in self._rules if r.name != rule_name]

    @property
    def rules(self) -> List:
        """Get the list of active rules."""
        return self._rules.copy()
