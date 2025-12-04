from typing import Dict, List
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic
from src.core import IStorageManager
from .cardinality_estimator import CardinalityEstimator
from typing import Optional
import math


class CostModel:
    
    def __init__(self, storage_manager: IStorageManager, 
                 block_size: int = 4096,
                 memory_size: int = 1000):
        self.storage_manager = storage_manager
        self.cardinality_estimator = CardinalityEstimator(storage_manager)
        self.block_size = block_size
        self.memory_size = memory_size
        
        # Cost constants (dalam unit I/O)
        self.SEQUENTIAL_READ_COST = 1
        self.RANDOM_READ_COST = 10
        self.WRITE_COST = 5
        self.CPU_TUPLE_COST = 0.01
    
    def get_cost(self, query_tree: QueryTree) -> float:
        return self.compute_node_cost(query_tree)
    
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
            # Default cost untuk node types lainnya
            child_cost = sum(self.compute_node_cost(child) for child in node.children)
            return child_cost + 1.0  # Small overhead
    
    def compute_table_scan_cost(self, node: QueryTree) -> float:
        table_name = self.extract_table_name(node.value)
        try:
            stats = self.storage_manager.get_stats(table_name)
            # Sequential read seluruh table
            return stats.b_r * self.SEQUENTIAL_READ_COST
        except Exception:
            return 1000  # Default cost
        
    def compute_selection_cost(self, node: QueryTree) -> float:
        if not node.children:
            return 0
        
        input_cost = self.compute_node_cost(node.children[0])
        input_cardinality = self.estimate_input_cardinality(node.children[0])
        
        # CPU cost untuk evaluasi kondisi
        cpu_cost = input_cardinality * self.CPU_TUPLE_COST
        
        return input_cost + cpu_cost
    
    def compute_projection_cost(self, node: QueryTree) -> float:
        if not node.children:
            return 0
        
        input_cost = self.compute_node_cost(node.children[0])
        input_cardinality = self.estimate_input_cardinality(node.children[0])
        
        # CPU cost untuk proyeksi
        cpu_cost = input_cardinality * self.CPU_TUPLE_COST * 0.5
        
        return input_cost + cpu_cost
    
    def compute_join_cost(self, node: QueryTree) -> float:
        if len(node.children) != 2:
            return 10000  # High cost untuk invalid join
        
        left_child, right_child = node.children
        left_cost = self.compute_node_cost(left_child)
        right_cost = self.compute_node_cost(right_child)
        
        left_card = self.estimate_input_cardinality(left_child)
        right_card = self.estimate_input_cardinality(right_child)
        
        # Pilih algoritma join berdasarkan statistics
        join_cost = self.estimate_join_algorithm_cost(
            left_child, right_child, left_card, right_card, node.value
        )
        
        return left_cost + right_cost + join_cost
    
    def estimate_join_algorithm_cost(self, left_node: QueryTree, right_node: QueryTree,
                                    left_card: float, right_card: float, 
                                    condition: str) -> float:
        left_stats = self.get_node_statistics(left_node)
        right_stats = self.get_node_statistics(right_node)
        
        # Coba nested loop join cost
        nl_cost = self.nested_loop_join_cost(left_card, right_card)
        
        # Coba hash join cost
        hash_cost = self.hash_join_cost(left_card, right_card)
        
        # Coba merge join cost (jika kondisi equijoin dan sorted)
        if left_stats and right_stats:
            merge_cost = self.merge_join_cost(left_card, right_card, condition, left_stats, right_stats)
        else:
            merge_cost = float('inf')
        
        return min(nl_cost, hash_cost, merge_cost)
    
    def nested_loop_join_cost(self, left_card: float, right_card: float) -> float:
        # Asumsi: left sebagai outer, right sebagai inner
        left_blocks = math.ceil(left_card / self.get_blocking_factor())
        right_blocks = math.ceil(right_card / self.get_blocking_factor())
        
        return left_blocks * right_blocks * self.SEQUENTIAL_READ_COST
    
    def hash_join_cost(self, left_card: float, right_card: float) -> float:
        left_blocks = math.ceil(left_card / self.get_blocking_factor())
        right_blocks = math.ceil(right_card / self.get_blocking_factor())
        
        # Build + probe phases
        build_cost = left_blocks * self.SEQUENTIAL_READ_COST
        probe_cost = right_blocks * self.SEQUENTIAL_READ_COST
        
        return build_cost + probe_cost
    
    def merge_join_cost(self, left_card: float, right_card: float, condition: str,
                        left_stats: Statistic, right_stats: Statistic) -> float:
        if not self.cardinality_estimator.is_equijoin(condition):
            return float('inf')  # Merge join hanya untuk equijoin
        
        left_blocks = math.ceil(left_card / self.get_blocking_factor())
        right_blocks = math.ceil(right_card / self.get_blocking_factor())
        
        # Cost sorting + merge
        sort_left = self.external_sort_cost(left_card)
        sort_right = self.external_sort_cost(right_card)
        merge_cost = (left_blocks + right_blocks) * self.SEQUENTIAL_READ_COST
        
        return sort_left + sort_right + merge_cost
    
    def compute_cartesian_product_cost(self, node: QueryTree) -> float:
        if len(node.children) != 2:
            return 10000
        
        left_child, right_child = node.children
        left_cost = self.compute_node_cost(left_child)
        right_cost = self.compute_node_cost(right_child)
        
        left_card = self.estimate_input_cardinality(left_child)
        right_card = self.estimate_input_cardinality(right_child)
        
        # Cartesian product sangat expensive
        product_cost = left_card * right_card * self.CPU_TUPLE_COST
        
        return left_cost + right_cost + product_cost
    
    def compute_sort_cost(self, node: QueryTree) -> float:
        if not node.children:
            return 0
        
        input_cost = self.compute_node_cost(node.children[0])
        input_cardinality = self.estimate_input_cardinality(node.children[0])
        
        sort_cost = self.external_sort_cost(input_cardinality)
        
        return input_cost + sort_cost
    
    def external_sort_cost(self, cardinality: float) -> float:
        blocks = math.ceil(cardinality / self.get_blocking_factor())
        passes = math.ceil(math.log(blocks / self.memory_size) / math.log(self.memory_size - 1))
        
        if passes < 1:
            passes = 1
            
        return 2 * blocks * passes * self.SEQUENTIAL_READ_COST
    
    def get_blocking_factor(self) -> float:
        return 100  # Default estimate
    
    def estimate_input_cardinality(self, node: QueryTree) -> float:
        # Ini adalah implementasi sederhana, bisa diperluas
        if node.type == "table":
            table_name = self.extract_table_name(node.value)
            try:
                stats = self.storage_manager.get_stats(table_name)
                return stats.n_r
            except Exception:
                return 1000
        
        # Untuk node lainnya, estimate berdasarkan children
        if node.children:
            return self.estimate_input_cardinality(node.children[0])
        
        return 1000
    
    def extract_table_name(self, value: str) -> str:
        if not value:
            return "unknown"
        
        # Handle table dengan alias: "table AS alias" atau "table alias"
        parts = value.split()
        if len(parts) >= 3 and parts[1].upper() == "AS":
            return parts[0]
        elif len(parts) >= 2:
            return parts[0]
        else:
            return parts[0] if parts else "unknown"
    
    def get_node_statistics(self, node: QueryTree) -> Optional[Statistic]:
        if node.type == "table":
            table_name = self.extract_table_name(node.value)
            try:
                return self.storage_manager.get_stats(table_name)
            except Exception:
                return None
        return None