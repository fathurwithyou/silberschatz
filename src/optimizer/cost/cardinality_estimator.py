from typing import Dict, List, Optional, Set, Tuple
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic, Condition, ComparisonOperator
from src.core import IStorageManager
import re
import math


class CardinalityEstimator:
    
    def __init__(self, statistics: Dict[str, Statistic], storage_manager: IStorageManager):
        self.statistics = statistics
        self.storage_manager = storage_manager
    
    def estimate_selection_cardinality(self, table_name: str, conditions: List[Condition]) -> float:
        stats = self.get_table_statistics(table_name)
        
        if not stats:
            return self.conservative_fallback_estimation(table_name, conditions)
        
        base_cardinality = stats.n_r
        
        if not conditions:
            return float(base_cardinality)
        
        # Hitung combined selectivity
        combined_selectivity = 1.0
        for condition in conditions:
            selectivity = self.estimate_condition_selectivity(stats, condition)
            combined_selectivity *= selectivity
        
        estimated_cardinality = base_cardinality * combined_selectivity
        
        # Minimal 1 row, maksimal all rows
        return max(1.0, min(estimated_cardinality, float(base_cardinality)))
    
    def get_table_statistics(self, table_name: str) -> Optional[Statistic]:
        if table_name in self.statistics:
            return self.statistics[table_name]
        
        try:
            return self.storage_manager.get_stats(table_name)
        except:
            return None
    
    def conservative_fallback_estimation(self, table_name: str, conditions: List[Condition]) -> float:
        # Estimasi kasar jika statistik tidak tersedia
        try:
            schema = self.storage_manager.get_table_schema(table_name)
            if schema and schema.columns:
                base_estimate = 1000.0
                if conditions:
                    selectivity = self.estimate_conservative_selectivity(conditions)
                    return base_estimate * selectivity
                return base_estimate
        except:
            pass
        
        return 1000.0 if not conditions else 100.0
    
    def estimate_conservative_selectivity(self, conditions: List[Condition]) -> float:
        # Selectivity default jika tidak ada statistik per kolom
        base_selectivity = 1.0
        for condition in conditions:
            if condition.operator == ComparisonOperator.EQ:
                base_selectivity *= 0.1
            elif condition.operator == ComparisonOperator.NE:
                base_selectivity *= 0.9
            else: # Range queries
                base_selectivity *= 0.3
        
        return max(0.01, base_selectivity) # Minimal 1% selectivity
    
    def estimate_condition_selectivity(self, stats: Statistic, condition: Condition) -> float:
        column = condition.column
        
        if column not in stats.V:
            return self.operator_based_fallback(condition.operator)
        
        V_A = stats.V[column]
        n_r = stats.n_r
        
        if V_A <= 0 or n_r <= 0:
            return self.operator_based_fallback(condition.operator)
        
        if condition.operator == ComparisonOperator.EQ:
            # Equality: 1/V(A), minimal 1/n_r
            return max(1.0 / V_A, 1.0 / n_r)
            
        elif condition.operator == ComparisonOperator.NE:
            # Inequality: 1 - 1/V(A)
            eq_selectivity = 1.0 / V_A
            return 1.0 - eq_selectivity
            
        elif condition.operator in [ComparisonOperator.GT, ComparisonOperator.LT]:
            # Range queries: basis 1/3, sesuaikan jika ada min/max
            base_selectivity = 0.33
            
            # Jika ada min/max, sesuaikan estimasi berdasarkan posisi nilai
            if stats.min_values and stats.max_values and column in stats.min_values:
                try:
                    condition_val = float(condition.value)
                    min_val = float(stats.min_values[column])
                    max_val = float(stats.max_values[column])
                    
                    if max_val > min_val:
                        position = (condition_val - min_val) / (max_val - min_val)
                        if condition.operator == ComparisonOperator.GT:
                            range_selectivity = 1.0 - position
                        else: # LT
                            range_selectivity = position
                        
                        # Campuran estimasi posisi dan baseline
                        base_selectivity = 0.7 * range_selectivity + 0.3 * base_selectivity
                except (ValueError, TypeError):
                    pass
            
            return max(0.01, min(0.99, base_selectivity))
            
        elif condition.operator in [ComparisonOperator.GE, ComparisonOperator.LE]:
            # GE/LE didekati dari GT/LT
            exclusive_op = ComparisonOperator.GT if condition.operator == ComparisonOperator.GE else ComparisonOperator.LT
            exclusive_selectivity = self.estimate_condition_selectivity(stats, 
                Condition(column, exclusive_op, condition.value))
            return min(1.0, exclusive_selectivity * 1.1)
        
        return self.operator_based_fallback(condition.operator)
    
    def operator_based_fallback(self, operator: ComparisonOperator) -> float:
        # Selectivity cadangan berdasarkan tipe operator
        selectivity_map = {
            ComparisonOperator.EQ: 0.05,
            ComparisonOperator.NE: 0.95,
            ComparisonOperator.GT: 0.25,
            ComparisonOperator.LT: 0.25,
            ComparisonOperator.GE: 0.30,
            ComparisonOperator.LE: 0.30
        }
        return selectivity_map.get(operator, 0.5)
    
    def estimate_join_cardinality(self, left_stats: Statistic, right_stats: Statistic, 
                                join_condition: str) -> float:
        left_card = float(left_stats.n_r)
        right_card = float(right_stats.n_r)
        
        if left_card <= 0 or right_card <= 0:
            return 0.0
        
        # Estimasi equijoin
        if self.is_equijoin(join_condition):
            join_columns = self.extract_join_columns(join_condition)
            if join_columns:
                left_col, right_col = join_columns
                left_col_name = self.extract_column_name(left_col)
                right_col_name = self.extract_column_name(right_col)
                
                V_left = left_stats.V.get(left_col_name, max(left_stats.n_r, 1))
                V_right = right_stats.V.get(right_col_name, max(right_stats.n_r, 1))
                
                # |R| * |S| / max(V(A,R), V(A,S))
                join_cardinality = (left_card * right_card) / max(V_left, V_right, 1)
                
                max_possible = left_card * right_card
                min_possible = max(left_card, right_card)
                return max(min_possible, min(join_cardinality, max_possible))
            
            # Jika pola tidak terdeteksis
            return min(left_card, right_card) * 2.0
        
        # Non-equijoin
        return left_card * right_card * 0.1
    
    def extract_column_name(self, column_ref: str) -> str:
        if '.' in column_ref:
            return column_ref.split('.')[-1]
        return column_ref
    
    def is_equijoin(self, condition: str) -> bool:
        if not condition:
            return False
        return '=' in condition and all(op not in condition for op in ['!=', '<', '>', '<=', '>='])
    
    def extract_join_columns(self, condition: str) -> Optional[Tuple[str, str]]:
        if not condition:
            return None
        
        # Pattern table.column = table.column
        pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        match = re.search(pattern, condition)
        
        if match:
            return (match.group(1), match.group(2))
        
        # Pattern nama kolom
        pattern_bare = r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)'
        match_bare = re.search(pattern_bare, condition)
        
        if match_bare:
            return (match_bare.group(1), match_bare.group(2))
        
        return None
    
    def estimate_projection_cardinality(self, input_cardinality: float) -> float:
        return input_cardinality
    
    def estimate_cartesian_product_cardinality(self, left_card: float, right_card: float) -> float:
        return left_card * right_card