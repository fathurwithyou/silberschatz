from typing import List, Tuple, Dict, Set, Optional
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic
from .cost_model import CostModel
import itertools
import math


class JoinOrderingOptimizer:
    
    def __init__(self, cost_model: CostModel, max_join_size: int = 6):
        self.cost_model = cost_model
        self.max_join_size = max_join_size
    
    def find_optimal_join_order(self, join_trees: List[QueryTree]) -> Optional[QueryTree]:
        if not join_trees:
            return None
        
        if len(join_trees) == 1:
            return join_trees[0]
        
        # Untuk join kecil, gunakan exhaustive search
        if len(join_trees) <= min(self.max_join_size, 4):
            return self.exhaustive_search(join_trees)
        else:
            # Untuk join besar, gunakan greedy algorithm
            return self.greedy_ordering(join_trees)
    
    def exhaustive_search(self, join_trees: List[QueryTree]) -> Optional[QueryTree]:
        if len(join_trees) == 1:
            return join_trees[0]
        
        best_tree = None
        best_cost = float('inf')
        
        # Coba berbagai kombinasi urutan join
        for perm in itertools.permutations(join_trees):
            current_tree = self.build_left_deep_tree(list(perm))
            current_cost = self.cost_model.get_cost(current_tree)
            
            if current_cost < best_cost:
                best_cost = current_cost
                best_tree = current_tree
        
        return best_tree
    
    def greedy_ordering(self, join_trees: List[QueryTree]) -> QueryTree:
        if not join_trees:
            return None
        
        tables_with_size = []
        for tree in join_trees:
            if tree.type == "table":
                size = self.cost_model.estimate_input_cardinality(tree)
                tables_with_size.append((tree, size))
            else:
                # Untuk node non-table, gunakan nilai default
                tables_with_size.append((tree, 1000.0))
        
        tables_with_size.sort(key=lambda x: x[1])
        current_trees = [t[0] for t in tables_with_size]
        
        # Greedy join ordering
        while len(current_trees) > 1:
            best_cost = float('inf')
            best_pair = None
            best_join = None
            
            for i in range(len(current_trees)):
                for j in range(i + 1, len(current_trees)):
                    left = current_trees[i]
                    right = current_trees[j]
                    
                    join_tree = QueryTree(
                        type="join",
                        value="",
                        children=[left, right],
                        parent=None
                    )
                    
                    cost = self.cost_model.get_cost(join_tree)
                    
                    if cost < best_cost:
                        best_cost = cost
                        best_pair = (i, j)
                        best_join = join_tree
            
            # Ganti dua node dengan hasil join-nya
            if best_join and best_pair:
                i, j = best_pair
                new_trees = [best_join]
                for idx, tree in enumerate(current_trees):
                    if idx != i and idx != j:
                        new_trees.append(tree)
                current_trees = new_trees
            else:
                break
        
        return current_trees[0] if current_trees else None
    
    def build_left_deep_tree(self, trees: List[QueryTree]) -> QueryTree:
        # Bentuk left-deep: (((A ▷◁ B) ▷◁ C) ▷◁ D)
        if len(trees) == 1:
            return trees[0]
        
        current = trees[0]
        for i in range(1, len(trees)):
            current = QueryTree(
                type="join",
                value="",
                children=[current, trees[i]],
                parent=None
            )
        
        return current