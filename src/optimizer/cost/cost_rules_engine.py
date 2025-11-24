from typing import List, Optional, Set
from src.core.models.query import QueryTree
from src.core.models.storage import TableSchema
from src.optimizer.rules.base_rule import OptimizationRule
from .cost_model import CostModel


class CostBasedRuleEngine:
    
    def __init__(self, cost_model: CostModel, max_iterations: int = 3):
        self.cost_model = cost_model
        self.max_iterations = max_iterations
    
    def apply_cost_based_rules(self, query_tree: QueryTree, 
                             rules: List[OptimizationRule]) -> QueryTree:
        current_tree = query_tree
        best_cost = self.cost_model.get_cost(current_tree)
        iterations = 0
        
        while iterations < self.max_iterations:
            iterations += 1
            improved = False
            
            for rule in rules:
                if rule.is_applicable(current_tree):
                    candidate = rule.apply(current_tree)
                    
                    # Hanya evaluasi jika menghasilkan tree baru
                    if candidate and candidate != current_tree:
                        candidate_cost = self.cost_model.get_cost(candidate)
                        
                        # Terapkan jika cost lebih rendah
                        if candidate_cost < best_cost:
                            current_tree = candidate
                            best_cost = candidate_cost
                            improved = True
                            break
            
            if not improved:
                break
        
        return current_tree


class CostBasedProjectionPushdown(OptimizationRule):
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
        
    @property
    def name(self) -> str:
        return "CostBasedProjectionPushdown"
    
    def is_applicable(self, node: QueryTree) -> bool:
        return (node.type == "projection" and 
                node.children and 
                len(node.children) == 1 and
                node.children[0].type in ["selection", "join"])
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        original_cost = self.cost_model.get_cost(node)
        child = node.children[0]
        
        if child.type == "selection":
            return self.push_through_selection(node, child, original_cost)
        elif child.type == "join":
            return self.push_through_join(node, child, original_cost)
        
        return None
    
    def push_through_selection(self, projection: QueryTree, selection: QueryTree, 
                              original_cost: float) -> Optional[QueryTree]:
    # Push projection di bawah selection
        if not selection.children:
            return None
        
        new_projection = QueryTree(
            type="projection",
            value=projection.value,
            children=selection.children.copy(),
            parent=None
        )
        
        new_selection = QueryTree(
            type="selection",
            value=selection.value,
            children=[new_projection],
            parent=None
        )
        
        new_projection.parent = new_selection
        
        candidate_cost = self.cost_model.get_cost(new_selection)
        
        if candidate_cost < original_cost:
            return new_selection
        
        return None
    
    def push_through_join(self, projection: QueryTree, join: QueryTree,
                          original_cost: float) -> Optional[QueryTree]:
        # Push projection di bawah join dengan analisis schema
        if len(join.children) != 2:
            return None
        
        left_child, right_child = join.children
        
        # Dapatkan schema untuk analisis kolom
        left_schema = self.get_table_schema(left_child)
        right_schema = self.get_table_schema(right_child)
        
        if not left_schema or not right_schema:
            return None
        
        # Analisis kolom projection
        projection_columns = self.parse_projection_columns(projection.value)
        
        if projection_columns == {"*"}:
            candidate = self.create_star_projection_pushdown(projection, join, left_child, right_child)
        else:
            candidate = self.create_specific_projection_pushdown(projection, join, left_child, right_child,
                                                                left_schema, right_schema, projection_columns)
        
        if candidate:
            candidate_cost = self.cost_model.get_cost(candidate)
            if candidate_cost < original_cost:
                return candidate
        
        return None
    
    def get_table_schema(self, node: QueryTree) -> Optional[TableSchema]:
        # Dapatkan TableSchema untuk node tabel
        try:
            table_name = self.cost_model.extract_table_name(node.value)
            return self.cost_model.storage_manager.get_table_schema(table_name)
        except:
            return None
    
    def parse_projection_columns(self, projection_value: str) -> Set[str]:
        # Parse kolom dari SELECT
        if not projection_value or projection_value.strip() == "*":
            return {"*"}
        
        columns = set()
        for col in projection_value.split(','):
            col = col.strip()
            if col:
                columns.add(col)
        return columns
    
    def create_star_projection_pushdown(self, projection: QueryTree, join: QueryTree,
                                       left_child: QueryTree, right_child: QueryTree) -> QueryTree:
        # Buat pushdown untuk SELECT *
        left_projection = QueryTree(
            type="projection",
            value="*",
            children=[left_child],
            parent=None
        )
        
        right_projection = QueryTree(
            type="projection", 
            value="*",
            children=[right_child],
            parent=None
        )
        
        new_join = QueryTree(
            type=join.type,
            value=join.value,
            children=[left_projection, right_projection],
            parent=None
        )
        
        left_projection.parent = new_join
        right_projection.parent = new_join
        
        return new_join
    
    def create_specific_projection_pushdown(self, projection: QueryTree, join: QueryTree,
                                           left_child: QueryTree, right_child: QueryTree,
                                           left_schema: TableSchema, right_schema: TableSchema,
                                           projection_columns: Set[str]) -> Optional[QueryTree]:
        
        left_table_name = self.cost_model.extract_table_name(left_schema.table_name)
        right_table_name = self.cost_model.extract_table_name(right_schema.table_name)
        
        left_columns, right_columns = self.distribute_columns_by_table(
            projection_columns, left_schema, right_schema, left_table_name, right_table_name
        )
        
        # Hanya buat jika ada kolom yang bisa di-pushdown
        if not left_columns and not right_columns:
            return None
        
        # Buat projection nodes
        left_projection_value = "*" if not left_columns else ", ".join(sorted(left_columns))
        left_projection = QueryTree(
            type="projection",
            value=left_projection_value,
            children=[left_child],
            parent=None
        )
        
        right_projection_value = "*" if not right_columns else ", ".join(sorted(right_columns))
        right_projection = QueryTree(
            type="projection",
            value=right_projection_value,
            children=[right_child],
            parent=None
        )
        
        new_join = QueryTree(
            type=join.type,
            value=join.value,
            children=[left_projection, right_projection],
            parent=None
        )
        
        left_projection.parent = new_join
        right_projection.parent = new_join
        
        return new_join
    
    def distribute_columns_by_table(self, projection_columns: Set[str],
                                   left_schema: TableSchema, right_schema: TableSchema,
                                   left_table_name: str, right_table_name: str) -> tuple[Set[str], Set[str]]:
        # Distribusikan kolom ke tabel berdasarkan schema analysis
        left_columns = set()
        right_columns = set()
        
        left_all_columns = {col.name for col in left_schema.columns}
        right_all_columns = {col.name for col in right_schema.columns}
        
        for column in projection_columns:
            if '.' in column:
                # Qualified column
                table_part, column_part = column.split('.', 1)
                if table_part == left_table_name and column_part in left_all_columns:
                    left_columns.add(column_part)
                elif table_part == right_table_name and column_part in right_all_columns:
                    right_columns.add(column_part)
                else:
                    # Tidak match schema, tetap tambahkan
                    left_columns.add(column)
                    right_columns.add(column)
            else:
                # Unqualified column
                if column in left_all_columns and column in right_all_columns:
                    left_columns.add(f"{left_table_name}.{column}")
                    right_columns.add(f"{right_table_name}.{column}")
                elif column in left_all_columns:
                    left_columns.add(column)
                elif column in right_all_columns:
                    right_columns.add(column)
                else:
                    # Tidak ditemukan, tetap tambahkan
                    left_columns.add(column)
                    right_columns.add(column)
        
        return left_columns, right_columns


class CostBasedJoinReordering(OptimizationRule):
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
        
    @property
    def name(self) -> str:
        return "CostBasedJoinReordering"
    
    def is_applicable(self, node: QueryTree) -> bool:
        return (node.type in ["join", "natural_join"] and 
                len(node.children) == 2 and
                self.are_both_tables(node.children))
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        original_cost = self.cost_model.get_cost(node)
        
        # Coba swap children
        left, right = node.children
        swapped = QueryTree(
            type=node.type,
            value=node.value,
            children=[right, left],
            parent=node.parent
        )
        
        swapped_cost = self.cost_model.get_cost(swapped)
        
        # Terapkan jika cost lebih rendah
        if swapped_cost < original_cost:
            return swapped
        
        return None
    
    def are_both_tables(self, children: List[QueryTree]) -> bool:
        return all(child.type == "table" for child in children)