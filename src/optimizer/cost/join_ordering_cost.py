from typing import List, Tuple, Dict, Set
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic
from .cost_model import CostModel
import itertools
import math


class JoinOrderingOptimizer:
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
    
    def find_optimal_join_order(self, join_trees: List[QueryTree]) -> QueryTree:
        if not join_trees:
            return None
        
        if len(join_trees) == 1:
            return join_trees[0]
        
        # Implementasi simplified dynamic programming
        best_tree = None
        best_cost = float('inf')
        
        # Coba berbagai kombinasi urutan join
        for left_size in range(1, len(join_trees)):
            for left_combo in itertools.combinations(join_trees, left_size):
                left_set = set(left_combo)
                right_set = [t for t in join_trees if t not in left_set]
                
                if not right_set:
                    continue
                
                # Bangun join tree untuk subset kiri
                left_tree = self.build_join_tree(list(left_set))
                # Bangun join tree untuk subset kanan  
                right_tree = self.build_join_tree(right_set)
                
                # Buat join node
                join_tree = QueryTree(
                    type="join",
                    value="",  # Condition akan di-set nanti
                    children=[left_tree, right_tree],
                    parent=None
                )
                
                # Estimate cost
                cost = self.cost_model.get_cost(join_tree)
                
                if cost < best_cost:
                    best_cost = cost
                    best_tree = join_tree
        
        return best_tree if best_tree else self.build_join_tree(join_trees)
    
    def build_join_tree(self, trees: List[QueryTree]) -> QueryTree:
        if len(trees) == 1:
            return trees[0]
        
        # Build left-deep tree sebagai fallback
        current = trees[0]
        for i in range(1, len(trees)):
            current = QueryTree(
                type="join",
                value="",
                children=[current, trees[i]],
                parent=None
            )
        
        return current
    
    def greedy_join_ordering(self, tables: List[str], join_conditions: List[Tuple]) -> List[str]:
        if not tables:
            return []
        
        # Start dengan table terkecil
        table_stats = {name: self.cost_model.statistics.get(name) for name in tables}
        current_set = [min(tables, key=lambda t: table_stats[t].n_r if table_stats[t] else float('inf'))]
        remaining = [t for t in tables if t not in current_set]
        
        while remaining:
            best_table = None
            best_cost = float('inf')
            
            for table in remaining:
                candidate_set = current_set + [table]
                cost = self.estimate_join_cost_for_set(candidate_set, join_conditions)
                
                if cost < best_cost:
                    best_cost = cost
                    best_table = table
            
            if best_table:
                current_set.append(best_table)
                remaining.remove(best_table)
            else:
                # Fallback: tambahkan table pertama yang tersisa
                current_set.append(remaining[0])
                remaining = remaining[1:]
        
        return current_set
    
    def estimate_join_cost_for_set(self, tables: List[str], 
                                  join_conditions: List[Tuple]) -> float:
        # Build temporary query tree untuk cost estimation
        table_trees = [
            QueryTree(type="table", value=table, children=[], parent=None)
            for table in tables
        ]
        
        join_tree = self.build_join_tree(table_trees)
        return self.cost_model.get_cost(join_tree)