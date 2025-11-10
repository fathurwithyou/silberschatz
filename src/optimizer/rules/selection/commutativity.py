from typing import Optional
from src.optimizer.rules.base_rule import OptimizationRule
from src.core.models.query import QueryTree, QueryNodeType

# Rule 2: Operasi seleksi bersifat komutatif
class SelectionCommutativityRule(OptimizationRule):
    @property
    def name(self) -> str:
        return "SelectionCommutativity"
    
    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != QueryNodeType.SELECTION:
            return False

        if not node.children or len(node.children) == 0:
            return False

        child = node.children[0]
        return child.type == QueryNodeType.SELECTION
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        # Reorder nested selections based on selectivity. More selective (lower estimated selectivity) conditions go first.
        if not self.is_applicable(node):
            return None
        
        child = node.children[0]
        
        # Estimate selectivity of both conditions
        parent_selectivity = self._estimate_selectivity(node.value)
        child_selectivity = self._estimate_selectivity(child.value)
        
        # If child is more selective, swap them
        if child_selectivity < parent_selectivity:
            # Create swapped structure
            new_inner = QueryTree(
                type=QueryNodeType.SELECTION,
                value=node.value,
                children=child.children.copy(),
                parent=None
            )

            new_outer = QueryTree(
                type=QueryNodeType.SELECTION,
                value=child.value,
                children=[new_inner],
                parent=None
            )
            
            new_inner.parent = new_outer
            if new_inner.children:
                for grandchild in new_inner.children:
                    grandchild.parent = new_inner
            
            return new_outer
        
        return None
    
    def _estimate_selectivity(self, condition: str) -> float:
        # Lower value = more selective
        if not condition:
            return 1.0
                
        # Check for equality (but not <> or !=)
        if '=' in condition and '<>' not in condition and '!=' not in condition and \
           '<=' not in condition and '>=' not in condition:
            return 0.1 
        
        # Check for range operators
        if any(op in condition for op in ['<', '>', '<=', '>=']):
            return 0.3
        
        # Check for not equal
        if '<>' in condition or '!=' in condition:
            return 0.9  
        
        return 0.5