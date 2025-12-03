from typing import List, Optional
from src.core.models import ParsedQuery
from src.core.models.query import QueryTree
from src.core.optimizer import IQueryOptimizer
from src.core.models.storage import Statistic
from src.core import IStorageManager
from .parser import QueryParser
from .cost.cost_model import CostModel
from .cost.cardinality_estimator import CardinalityEstimator
from .cost.cost_rules_engine import CostBasedRuleEngine, CostBasedProjectionPushdown, CostBasedJoinReordering
from .rules import (
    JoinCommutativityRule,
    JoinAssociativityRule,
    ProjectionEliminationRule,
    SelectionCartesianProductRule,
    SelectionCommutativityRule,
    SelectionDecompositionRule,
    SelectionThetaJoinRule,
)


class CostBasedOptimizer(IQueryOptimizer):
    
    def __init__(self, storage_manager: IStorageManager, max_iterations: int = 10):
        self.storage_manager = storage_manager
        self.max_iterations = max_iterations
        self.parser = QueryParser()
        
        # Initialize cost model dan components
        self.cost_model = CostModel(storage_manager)
        self.cardinality_estimator = CardinalityEstimator(storage_manager)
        self.rule_engine = CostBasedRuleEngine(self.cost_model)
        
        # Cost-based rules
        self.cost_based_rules = [
            CostBasedProjectionPushdown(self.cost_model),
            CostBasedJoinReordering(self.cost_model),
        ]
        
        # Logical rules (selalu diterapkan)
        self.logical_rules = [
            JoinCommutativityRule(),
            JoinAssociativityRule(prefer_right_deep=True),
            ProjectionEliminationRule(),
            SelectionCartesianProductRule(),
            SelectionCommutativityRule(),
            SelectionDecompositionRule(),
            SelectionThetaJoinRule(),
        ]
    
    def parse_query(self, query: str) -> ParsedQuery:
        return self.parser(query)
    
    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        current_tree = query.tree
        
        # Step 1: Terapkan logical rules
        current_tree = self.apply_logical_optimization(current_tree)
        
        # Step 2: Terapkan cost-based rules
        current_tree = self.rule_engine.apply_cost_based_rules(
            current_tree, self.cost_based_rules
        )
        
        return ParsedQuery(tree=current_tree, query=query.query)
    
    def get_cost(self, query: ParsedQuery) -> int:
        return int(self.cost_model.get_cost(query.tree))
    
    def apply_logical_optimization(self, tree: QueryTree) -> QueryTree:
        current_tree = tree
        iteration = 0
        
        while iteration < self.max_iterations:
            iteration += 1
            optimized = self.apply_rules_once(current_tree, self.logical_rules)
            
            if optimized is current_tree:
                break
                
            current_tree = optimized
        
        return current_tree
    
    def apply_rules_once(self, tree: QueryTree, rules: List) -> QueryTree:
        # Optimize children terlebih dahulu
        optimized_children = []
        children_changed = False
        
        for child in tree.children:
            optimized_child = self.apply_rules_once(child, rules)
            optimized_children.append(optimized_child)
            if optimized_child is not child:
                children_changed = True
        
        # Create new node jika children berubah
        if children_changed:
            tree = QueryTree(
                type=tree.type,
                value=tree.value,
                children=optimized_children,
                parent=tree.parent
            )
            for child in tree.children:
                child.parent = tree
        
        # Terapkan rules ke current node
        for rule in rules:
            if rule.is_applicable(tree):
                optimized = rule.apply(tree)
                if optimized is not None:
                    return optimized
        
        return tree