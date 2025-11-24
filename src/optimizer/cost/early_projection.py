from typing import Optional, Set, List, Dict
from src.core.models.query import QueryTree
from src.core.models.storage import TableSchema
from src.optimizer.rules.base_rule import OptimizationRule
from .cost_model import CostModel


class EarlyProjectionRule(OptimizationRule):
    
    def __init__(self, cost_model: CostModel):
        self.cost_model = cost_model
    
    @property
    def name(self) -> str:
        return "EarlyProjection"
    
    def is_applicable(self, node: QueryTree) -> bool:
        # Projection tepat di atas selection/join
        if node.type != "projection":
            return False
        if not node.children or len(node.children) != 1:
            return False
        return node.children[0].type in ["selection", "join"]
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        original_cost = self.cost_model.get_cost(node)
        child = node.children[0]
        
        candidate = None
        
        if child.type == "selection":
            candidate = self.push_through_selection(node, child)
        elif child.type == "join":
            candidate = self.push_through_join(node, child)
        
        if candidate and candidate != node:
            candidate_cost = self.cost_model.get_cost(candidate)
            
            if candidate_cost < original_cost:
                return candidate
        
        return None
    
    def push_through_selection(self, projection: QueryTree, selection: QueryTree) -> Optional[QueryTree]:
        # Projection dipindah ke bawah selection
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
        return new_selection
    
    def push_through_join(self, projection: QueryTree, join: QueryTree) -> Optional[QueryTree]:
        if len(join.children) != 2:
            return None
        
        left_child, right_child = join.children
        left_schema = self.get_table_schema(left_child)
        right_schema = self.get_table_schema(right_child)
        
        if not left_schema or not right_schema:
            return None
        
        # Analisis kolom projection
        projection_columns = self.parse_projection_columns(projection.value)
        
        if projection_columns == {"*"}:
            # SELECT * - push projection ke kedua sisi
            return self.push_star_projection(projection, join, left_child, right_child)
        else:
            # Analisis dan pisahkan berdasarkan tabel
            return self.push_specific_projection(projection, join, left_child, right_child, 
                                                left_schema, right_schema, projection_columns)
    
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
    
    def push_star_projection(self, projection: QueryTree, join: QueryTree, 
                            left_child: QueryTree, right_child: QueryTree) -> QueryTree:
        # Push SELECT * ke kedua sisi join
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
    
    def push_specific_projection(self, projection: QueryTree, join: QueryTree,
                                left_child: QueryTree, right_child: QueryTree,
                                left_schema: TableSchema, right_schema: TableSchema,
                                projection_columns: Set[str]) -> Optional[QueryTree]:
        
        # Pisahkan kolom berdasarkan tabel asalnya
        left_columns, right_columns = self.distribute_columns_by_table(
            projection_columns, left_schema, right_schema
        )
        
        # Hanya buat projection jika ada kolom yang relevan
        if not left_columns and not right_columns:
            return None
        
        # Buat projection untuk left side
        left_projection_value = "*" if not left_columns else ", ".join(sorted(left_columns))
        left_projection = QueryTree(
            type="projection",
            value=left_projection_value,
            children=[left_child],
            parent=None
        )
        
        # Buat projection untuk right side  
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
                                   left_schema: TableSchema, right_schema: TableSchema) -> tuple[Set[str], Set[str]]:
        # Distribusikan kolom ke tabel yang sesuai berdasarkan schema
        left_table_name = self.cost_model.extract_table_name(left_schema.table_name)
        right_table_name = self.cost_model.extract_table_name(right_schema.table_name)
        
        left_columns = set()
        right_columns = set()
        
        left_all_columns = {col.name for col in left_schema.columns}
        right_all_columns = {col.name for col in right_schema.columns}
        
        for column in projection_columns:
            if '.' in column:
                # Qualified column: table.column
                table_part, column_part = column.split('.', 1)
                if table_part == left_table_name and column_part in left_all_columns:
                    left_columns.add(column_part)
                elif table_part == right_table_name and column_part in right_all_columns:
                    right_columns.add(column_part)
                else:
                    # Jika tidak match, tetap tambahkan
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
                    # Tidak ditemukan di schema, tetap tambahkan
                    left_columns.add(column)
                    right_columns.add(column)
        
        return left_columns, right_columns