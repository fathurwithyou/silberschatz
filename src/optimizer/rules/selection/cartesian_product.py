from typing import Optional
from src.optimizer.rules.base_rule import OptimizationRule
from src.core.models.query import QueryTree

# Rule 4a: Operasi seleksi dapat digabungkan dengan hasil Cartesian product.
class SelectionCartesianProductRule(OptimizationRule):
    @property
    def name(self) -> str:
        return "SelectionCartesianProduct"
    
    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != "SELECTION":
            return False
        
        if not node.children or len(node.children) == 0:
            return False
        
        child = node.children[0]
        return child.type in ["CARTESIAN_PRODUCT", "CROSS_JOIN", "CARTESIAN"]
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        cartesian_node = node.children[0]
        
        if len(cartesian_node.children) < 2:
            return None
        
        # Create theta join with the selection condition
        theta_join = QueryTree(
            type="THETA_JOIN",
            value=node.value,  # Selection condition becomes join condition
            children=cartesian_node.children.copy(),
            parent=None
        )
        
        # Update parent references
        for child in theta_join.children:
            child.parent = theta_join
        
        return theta_join