from typing import List, Optional
from src.core.models.query import QueryTree
from src.optimizer.rules.base_rule import OptimizationRule
from .cost_model import CostModel


class CostBasedRuleEngine:
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
    
    def apply_cost_based_rules(self, query_tree: QueryTree, 
                             rules: List[OptimizationRule]) -> QueryTree:
        original_cost = self.cost_model.get_cost(query_tree)
        current_tree = query_tree
        improved = True
        
        while improved:
            improved = False
            
            for rule in rules:
                if rule.is_applicable(current_tree):
                    candidate = rule.apply(current_tree)
                    
                    if candidate and candidate != current_tree:
                        candidate_cost = self.cost_model.get_cost(candidate)
                        
                        # Terapkan hanya jika cost berkurang
                        if candidate_cost < original_cost:
                            current_tree = candidate
                            original_cost = candidate_cost
                            improved = True
                            break
        
        return current_tree


class CostBasedProjectionPushdown(OptimizationRule):
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
        self.base_rule = ProjectionPushdownRule()
    
    @property
    def name(self) -> str:
        return "CostBasedProjectionPushdown"
    
    def is_applicable(self, node: QueryTree) -> bool:
        return self.base_rule.is_applicable(node)
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        # Dapatkan candidate dari base rule
        candidate = self.base_rule.apply(node)
        if not candidate:
            return None
        
        # Bandingkan cost
        original_cost = self.cost_model.get_cost(node)
        candidate_cost = self.cost_model.get_cost(candidate)
        
        # Terapkan hanya jika cost berkurang
        if candidate_cost < original_cost:
            return candidate
        
        return None


class CostBasedJoinReordering(OptimizationRule):
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
        self.join_optimizer = JoinOrderingOptimizer(cost_model)
    
    @property
    def name(self) -> str:
        return "CostBasedJoinReordering"
    
    def is_applicable(self, node: QueryTree) -> bool:
        return node.type in ["join", "natural_join"] and len(node.children) >= 2
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        # Collect semua table nodes dalam subtree join
        table_nodes = self.collect_table_nodes(node)
        
        if len(table_nodes) < 2:
            return None
        
        # Dapatkan urutan join optimal
        optimal_tree = self.join_optimizer.find_optimal_join_order(table_nodes)
        
        if optimal_tree and optimal_tree != node:
            # Bandingkan cost
            original_cost = self.cost_model.get_cost(node)
            new_cost = self.cost_model.get_cost(optimal_tree)
            
            if new_cost < original_cost:
                return optimal_tree
        
        return None
    
    def collect_table_nodes(self, node: QueryTree) -> List[QueryTree]:
        tables = []
        
        if node.type == "table":
            tables.append(node)
        else:
            for child in node.children:
                tables.extend(self.collect_table_nodes(child))
        
        return tables