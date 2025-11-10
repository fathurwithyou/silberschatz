from typing import Dict, List, Optional, Set
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic, Condition, ComparisonOperator
import re
import math


class CardinalityEstimator:
    
    def __init__(self, statistics: Dict[str, Statistic]):
        self.statistics = statistics
    
    def estimate_selection_cardinality(self, table_name: str, conditions: List[Condition]) -> float:
        if table_name not in self.statistics:
            return 1000  # Default estimate
        
        stats = self.statistics[table_name]
        base_cardinality = stats.n_r
        
        if not conditions:
            return base_cardinality
        
        # Hitung combined selectivity
        combined_selectivity = 1.0
        for condition in conditions:
            selectivity = self.estimate_condition_selectivity(stats, condition)
            combined_selectivity *= selectivity
        
        return base_cardinality * combined_selectivity
    
    def estimate_condition_selectivity(self, stats: Statistic, condition: Condition) -> float:
        column = condition.column
        
        # Jika kolom tidak ada di statistics, return default
        if column not in stats.V:
            return self.default_selectivity(condition.operator)
        
        V_A = stats.V[column]  # Number of distinct values
        
        if condition.operator == ComparisonOperator.EQ:
            # Equality: 1/V(A)
            return 1.0 / V_A if V_A > 0 else 0.001
            
        elif condition.operator in [ComparisonOperator.NE]:
            # Inequality: 1 - 1/V(A)
            return 1.0 - (1.0 / V_A) if V_A > 0 else 0.999
            
        elif condition.operator in [ComparisonOperator.GT, ComparisonOperator.LT, 
                                  ComparisonOperator.GE, ComparisonOperator.LE]:
            # Range queries: asumsi 1/3
            return 0.33
            
        else:
            return 0.5
    
    def default_selectivity(self, operator: ComparisonOperator) -> float:
        selectivity_map = {
            ComparisonOperator.EQ: 0.1,
            ComparisonOperator.NE: 0.9,
            ComparisonOperator.GT: 0.33,
            ComparisonOperator.LT: 0.33,
            ComparisonOperator.GE: 0.33,
            ComparisonOperator.LE: 0.33
        }
        return selectivity_map.get(operator, 0.5)
    
    def estimate_join_cardinality(self, left_stats: Statistic, right_stats: Statistic, 
                                join_condition: str) -> float:
        # Jika natural join atau equijoin
        if self.is_equijoin(join_condition):
            left_card = left_stats.n_r
            right_card = right_stats.n_r
            
            # Cari kolom yang di-join dari kondisi
            join_columns = self.extract_join_columns(join_condition)
            if join_columns:
                left_col, right_col = join_columns
                V_left = left_stats.V.get(left_col, max(left_stats.n_r, 1))
                V_right = right_stats.V.get(right_col, max(right_stats.n_r, 1))
                
                # Formula: |R| * |S| / max(V(A,R), V(A,S))
                return (left_card * right_card) / max(V_left, V_right, 1)
            
            # Default untuk equijoin tanpa info kolom
            return min(left_card, right_card)
        
        # Untuk theta join non-equi, estimate lebih konservatif
        return left_stats.n_r * right_stats.n_r * 0.1
    
    def is_equijoin(self, condition: str) -> bool:
        if not condition:
            return False
        return '=' in condition and '!' not in condition and '<' not in condition and '>' not in condition
    
    def extract_join_columns(self, condition: str) -> Optional[tuple]:
        if not condition:
            return None
        
        # Pattern untuk match table.column = table.column
        pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        match = re.search(pattern, condition)
        
        if match:
            return (match.group(1), match.group(2))
        
        # Pattern untuk match bare column names
        pattern_bare = r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)'
        match_bare = re.search(pattern_bare, condition)
        
        if match_bare:
            return (match_bare.group(1), match_bare.group(2))
        
        return None
    
    def estimate_projection_cardinality(self, input_cardinality: float) -> float:
        return input_cardinality
    
    def estimate_cartesian_product_cardinality(self, left_card: float, right_card: float) -> float:
        return left_card * right_card