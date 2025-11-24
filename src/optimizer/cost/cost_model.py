from typing import Dict, List, Optional
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic
from src.core import IStorageManager
from .cardinality_estimator import CardinalityEstimator
import math


class CostModel:
    
    def __init__(self, statistics: Dict[str, Statistic], storage_manager: IStorageManager):
        self.statistics = statistics
        self.storage_manager = storage_manager
        self.cardinality_estimator = CardinalityEstimator(statistics, storage_manager)
        
        self.block_size = self.get_system_block_size()
        self.buffer_pool_size = self.get_buffer_pool_size()
        
        self.SEQUENTIAL_READ_COST = 1.0      # Base unit
        self.RANDOM_READ_COST = 10.0         # 10x sequential
        self.WRITE_COST = 5.0                # 5x sequential read
        self.CPU_TUPLE_COST = 0.001          # Relative CPU cost per tuple
        self.CPU_PREDICATE_COST = 0.0001     # Cost for predicate evaluation
    
    def get_system_block_size(self) -> int:
        return 4096 # 4KB standard block size
    
    def get_buffer_pool_size(self) -> int:
        return 100 # 100 blocks buffer pool
    
    def get_cost(self, query_tree: QueryTree) -> float:
        try:
            return self.compute_node_cost(query_tree)
        except Exception as e:
            print(f"Cost calculation failed: {e}")
            return float('inf')
    
    def compute_node_cost(self, node: QueryTree) -> float:
        if node.type == "table":
            return self.compute_table_scan_cost(node)
        elif node.type == "selection":
            return self.compute_selection_cost(node)
        elif node.type == "projection":
            return self.compute_projection_cost(node)
        elif node.type in ["join", "natural_join"]:
            return self.compute_join_cost(node)
        elif node.type == "cartesian_product":
            return self.compute_cartesian_product_cost(node)
        elif node.type == "order_by":
            return self.compute_sort_cost(node)
        else:
            # Untuk node type yang tidak dikenali, return infinity
            print(f"Warning: Unknown node type '{node.type}' in cost calculation")
            return float('inf')
    
    def compute_table_scan_cost(self, node: QueryTree) -> float:
        table_name = self.extract_table_name(node.value)
        
        try:
            stats = self.get_table_statistics(table_name)
            if stats:
                # Full scan
                return stats.b_r * self.SEQUENTIAL_READ_COST
        except:
            pass
        
        return 1000.0 * self.SEQUENTIAL_READ_COST
    
    def compute_selection_cost(self, node: QueryTree) -> float:
        if not node.children:
            return 0.0
        
        input_cost = self.compute_node_cost(node.children[0])
        input_cardinality = self.estimate_input_cardinality(node.children[0])
        
        # CPU cost untuk evaluasi kondisi
        cpu_cost = input_cardinality * self.CPU_PREDICATE_COST
        
        return input_cost + cpu_cost
    
    def compute_projection_cost(self, node: QueryTree) -> float:
        if not node.children:
            return 0.0
        
        input_cost = self.compute_node_cost(node.children[0])
        input_cardinality = self.estimate_input_cardinality(node.children[0])
        
        # CPU cost untuk proyeksi
        cpu_cost = input_cardinality * self.CPU_PREDICATE_COST * 0.5
        
        return input_cost + cpu_cost
    
    def compute_join_cost(self, node: QueryTree) -> float:
        if len(node.children) != 2:
            return float('inf') # Invalid join
        
        left_child, right_child = node.children
        left_cost = self.compute_node_cost(left_child)
        right_cost = self.compute_node_cost(right_child)
        
        left_card = self.estimate_input_cardinality(left_child)
        right_card = self.estimate_input_cardinality(right_child)
        
        # Pilih algoritma join paling murah
        join_cost = self.estimate_join_algorithm_cost(
            left_child, right_child, left_card, right_card, node.value
        )
        
        return left_cost + right_cost + join_cost
    
    def estimate_join_algorithm_cost(self, left_node: QueryTree, right_node: QueryTree,
                                   left_card: float, right_card: float, 
                                   condition: str) -> float:
        
        left_blocks = self.estimate_blocks(left_card, left_node)
        right_blocks = self.estimate_blocks(right_card, right_node)
        
        algorithms = []
        
        # Coba nested loop join cost
        nl_cost = self.nested_loop_join_cost(left_blocks, right_blocks)
        algorithms.append(nl_cost)
        
        # Coba hash join cost
        hash_cost = self.hash_join_cost(left_blocks, right_blocks, left_card, right_card)
        algorithms.append(hash_cost)
        
        # Coba merge join cost (jika kondisi equijoin)
        merge_cost = self.merge_join_cost(left_blocks, right_blocks, left_card, right_card, condition)
        algorithms.append(merge_cost)
        
        # Return minimum cost
        return min(algorithms)
    
    def estimate_blocks(self, cardinality: float, node: QueryTree) -> float:
        # Jika tabel asli punya f_r (blocking factor)
        if node.type == "table":
            table_name = self.extract_table_name(node.value)
            try:
                stats = self.get_table_statistics(table_name)
                if stats and stats.f_r > 0:
                    return math.ceil(cardinality / stats.f_r)
            except:
                pass
        
        avg_blocking_factor = self.get_average_blocking_factor()
        return math.ceil(cardinality / avg_blocking_factor) if avg_blocking_factor > 0 else cardinality
    
    def get_average_blocking_factor(self) -> float:
        if not self.statistics:
            return 100.0 # Default
        
        total_f_r = 0.0
        count = 0
        
        for stats in self.statistics.values():
            if stats.f_r > 0:
                total_f_r += stats.f_r
                count += 1
        
        return total_f_r / count if count > 0 else 100.0
    
    def nested_loop_join_cost(self, left_blocks: float, right_blocks: float) -> float:
        return left_blocks * right_blocks * self.SEQUENTIAL_READ_COST
    
    def hash_join_cost(self, left_blocks: float, right_blocks: float, 
                      left_card: float, right_card: float) -> float:
        if left_blocks > self.buffer_pool_size * 0.8:
            return float('inf')
        
        # Build + probe phases
        build_cost = left_blocks * self.SEQUENTIAL_READ_COST
        probe_cost = right_blocks * self.SEQUENTIAL_READ_COST
        
        # Hash operation cost
        hash_ops_cost = (left_card + right_card) * self.CPU_TUPLE_COST
        
        return build_cost + probe_cost + hash_ops_cost
    
    def merge_join_cost(self, left_blocks: float, right_blocks: float,
                       left_card: float, right_card: float, condition: str) -> float:
        if not self.cardinality_estimator.is_equijoin(condition):
            return float('inf') # Merge join jika equijoin
        
        # Cost sorting + merge
        sort_left = self.external_sort_cost(left_card)
        sort_right = self.external_sort_cost(right_card)
        merge_cost = (left_blocks + right_blocks) * self.SEQUENTIAL_READ_COST
        
        return sort_left + sort_right + merge_cost
    
    def compute_cartesian_product_cost(self, node: QueryTree) -> float:
        if len(node.children) != 2:
            return float('inf')
        
        left_child, right_child = node.children
        left_cost = self.compute_node_cost(left_child)
        right_cost = self.compute_node_cost(right_child)
        
        left_card = self.estimate_input_cardinality(left_child)
        right_card = self.estimate_input_cardinality(right_child)
        
        product_cost = left_card * right_card * self.CPU_TUPLE_COST * 10
        
        return left_cost + right_cost + product_cost
    
    def compute_sort_cost(self, node: QueryTree) -> float:
        if not node.children:
            return 0.0
        
        input_cost = self.compute_node_cost(node.children[0])
        input_cardinality = self.estimate_input_cardinality(node.children[0])
        
        sort_cost = self.external_sort_cost(input_cardinality)
        
        return input_cost + sort_cost
    
    def external_sort_cost(self, cardinality: float) -> float:
        blocks = math.ceil(cardinality / self.get_average_blocking_factor())
        
        if blocks <= self.buffer_pool_size:
            # Internal sort
            return 2 * blocks * self.SEQUENTIAL_READ_COST # read + write
        else:
            # External sort
            passes = math.ceil(math.log(blocks / self.buffer_pool_size) / 
                             math.log(self.buffer_pool_size - 1))
            passes = max(1, passes)
            return 2 * blocks * passes * self.SEQUENTIAL_READ_COST
    
    def estimate_input_cardinality(self, node: QueryTree) -> float:
        try:
            if node.type == "table":
                table_name = self.extract_table_name(node.value)
                stats = self.get_table_statistics(table_name)
                return float(stats.n_r) if stats else 1000.0
            
            elif node.type == "selection":
                if node.children:
                    input_card = self.estimate_input_cardinality(node.children[0])
                    return input_card * 0.5
            
            elif node.type == "join":
                if len(node.children) == 2:
                    left_card = self.estimate_input_cardinality(node.children[0])
                    right_card = self.estimate_input_cardinality(node.children[1])
                    return min(left_card, right_card) * 2.0
            
            # Untuk node lainnya, estimate berdasarkan children
            if node.children:
                return self.estimate_input_cardinality(node.children[0])
                
        except Exception as e:
            print(f"Cardinality estimation failed for node {node.type}: {e}")
        
        return 1000.0
    
    def get_table_statistics(self, table_name: str) -> Optional[Statistic]:
        if table_name in self.statistics:
            return self.statistics[table_name]
        
        try:
            return self.storage_manager.get_stats(table_name)
        except:
            return None
    
    def extract_table_name(self, value: str) -> str:
        if not value:
            return "unknown"
        
        parts = value.split()
        if len(parts) >= 3 and parts[1].upper() == "AS":
            return parts[0]
        elif len(parts) >= 2:
            return parts[0]
        else:
            return parts[0] if parts else "unknown"