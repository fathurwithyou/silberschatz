from typing import Optional, List
from src.optimizer.rules.base_rule import OptimizationRule
from src.core.models.query import QueryTree

# Rule 1: Operasi seleksi konjungtif dapat diuraikan menjadi urutan seleksi.
class SelectionDecompositionRule(OptimizationRule):
    @property
    def name(self) -> str:
        return "SelectionDecomposition"
    
    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != "SELECTION":
            return False
        
        if node.value and ' AND ' in node.value.upper():
            return True
        
        return False
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        # Decompose conjunctive selection into nested selections.
        if not self.is_applicable(node):
            return None
        
        # Extract AND conditions
        conditions = self._split_and_conditions(node.value)
        
        if len(conditions) <= 1:
            return None
        
        # Build nested selections from innermost to outermost
        if not node.children or len(node.children) == 0:
            return None
        
        current_tree = node.children[0]
        
        # Create nested selections from bottom up
        for condition in reversed(conditions):
            new_selection = QueryTree(
                type="SELECTION",
                value=condition.strip(),
                children=[current_tree],
                parent=None
            )
            current_tree.parent = new_selection
            current_tree = new_selection
        
        return current_tree
    
    def _split_and_conditions(self, condition: str) -> List[str]:
        # Split condition by AND operator
        import re
        # Split by AND (case insensitive) while preserving parentheses
        parts = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
        return [part.strip() for part in parts if part.strip()]