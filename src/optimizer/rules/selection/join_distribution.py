from typing import Optional, List, Set
from src.optimizer.rules.base_rule import OptimizationRule
from src.core.models.query import QueryTree, QueryNodeType
from src.core import IStorageManager
import re

# Rule 7: Operasi seleksi dapat terdistribusi terhadap operasi theta join
class SelectionJoinDistributionRule(OptimizationRule):   
    def __init__(self, storage_manager: IStorageManager):
        if storage_manager is None:
            raise ValueError(
                "SelectionJoinDistributionRule requires a valid storage_manager. "
            )
        self._storage_manager = storage_manager
    
    @property
    def name(self) -> str:
        return "SelectionJoinDistribution"
    
    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != QueryNodeType.SELECTION:
            return False

        if not node.children or len(node.children) == 0:
            return False

        child = node.children[0]
        return child.type in [QueryNodeType.JOIN, QueryNodeType.THETA_JOIN, QueryNodeType.NATURAL_JOIN, "INNER_JOIN"]
    
    def apply(self, node: QueryTree) -> QueryTree:
        if not self.is_applicable(node):
            return None
        
        join_node = node.children[0]
        
        if len(join_node.children) < 2:
            return None
        
        left_child = join_node.children[0]
        right_child = join_node.children[1]
        
        # Get table names from left and right relations
        left_tables = self._get_table_names(left_child)
        right_tables = self._get_table_names(right_child)
        
        # Get column names from schema if storage_manager available
        left_columns = self._get_table_columns(left_tables)
        right_columns = self._get_table_columns(right_tables)
        
        # Split selection conditions by AND
        conditions = self._split_and_conditions(node.value)
        
        # Classify conditions based on which relation they reference
        left_conditions = []
        right_conditions = []
        both_conditions = []
        
        for cond in conditions:
            tables_in_cond = self._extract_table_references(cond)
            columns_in_cond = self._extract_column_references(cond)
            
            # Determine where condition belongs
            refs_left = False
            refs_right = False
            
            # Check table references
            if tables_in_cond:
                refs_left = bool(tables_in_cond & left_tables)
                refs_right = bool(tables_in_cond & right_tables)
            
            # Check column references (if no table prefix)
            if not tables_in_cond and columns_in_cond:
                refs_left = bool(columns_in_cond & left_columns)
                refs_right = bool(columns_in_cond & right_columns)
            
            # Classify condition
            if refs_left and not refs_right:
                left_conditions.append(cond)
            elif refs_right and not refs_left:
                right_conditions.append(cond)
            else:
                both_conditions.append(cond)
        
        # If no conditions can be pushed, return None
        if not left_conditions and not right_conditions:
            return None
        
        # Build optimized tree
        new_left = self._apply_selections(left_child, left_conditions)
        new_right = self._apply_selections(right_child, right_conditions)
        
        # Create new join node
        new_join = QueryTree(
            type=join_node.type,
            value=join_node.value,
            children=[new_left, new_right],
            parent=None
        )
        new_left.parent = new_join
        new_right.parent = new_join
        
        # If there are conditions on both relations, wrap with selection
        if both_conditions:
            combined_condition = ' AND '.join(both_conditions)
            result = QueryTree(
                type=QueryNodeType.SELECTION,
                value=combined_condition,
                children=[new_join],
                parent=None
            )
            new_join.parent = result
            return result
        
        return new_join
    
    def _split_and_conditions(self, condition: str) -> List[str]:
        parts = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
        return [part.strip() for part in parts if part.strip()]
    
    def _get_table_names(self, node: QueryTree) -> Set[str]:
        tables = set()

        if node.type in ["TABLE_SCAN", QueryNodeType.TABLE, "SCAN"]:
            if node.value:
                # Value might be "table_name" or "table_name AS alias"
                parts = node.value.split()
                tables.add(parts[0])
                # If there's an alias (AS), add it too
                if len(parts) >= 3 and parts[1].upper() == "AS":
                    tables.add(parts[2])
                elif len(parts) == 2:
                    tables.add(parts[1])
        
        # Recursively check children
        if node.children:
            for child in node.children:
                tables.update(self._get_table_names(child))
        
        return tables
    
    def _get_table_columns(self, table_names: Set[str]) -> Set[str]:
        columns = set()
        
        if not self._storage_manager:
            return columns
        
        for table_name in table_names:
            try:
                schema = self._storage_manager.get_table_schema(table_name)
                if schema:
                    for col_def in schema.columns:
                        columns.add(col_def.name)
            except:
                # If schema not found, continue
                pass
        
        return columns
    
    def _extract_table_references(self, condition: str) -> Set[str]:
        tables = set()
        
        # Pattern to match table.column references
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\.'
        matches = re.findall(pattern, condition)
        
        tables.update(matches)
        return tables
    
    def _extract_column_references(self, condition: str) -> Set[str]:
        # Extract column names (without table prefix) from condition.
        columns = set()
        
        # Remove table prefixes first
        condition_no_prefix = re.sub(r'[a-zA-Z_][a-zA-Z0-9_]*\.', '', condition)
        
        # Extract identifiers (potential column names)
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        matches = re.findall(pattern, condition_no_prefix)
        
        # Filter out SQL keywords and operators
        keywords = {'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 
                   'TRUE', 'FALSE', 'AS', 'ON'}
        
        for match in matches:
            if match.upper() not in keywords:
                # Check if it's not a number
                try:
                    float(match)
                except ValueError:
                    # Check if it's not a string literal
                    if not (match.startswith("'") or match.startswith('"')):
                        columns.add(match)
        
        return columns
    
    def _apply_selections(self, node: QueryTree, conditions: List[str]) -> QueryTree:
        if not conditions:
            return node

        current = node
        for condition in conditions:
            new_selection = QueryTree(
                type=QueryNodeType.SELECTION,
                value=condition,
                children=[current],
                parent=None
            )
            current.parent = new_selection
            current = new_selection
        
        return current