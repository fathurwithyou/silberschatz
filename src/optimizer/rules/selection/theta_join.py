from typing import Optional
from src.optimizer.rules.base_rule import OptimizationRule
from src.core.models.query import QueryTree, QueryNodeType

# Rule 4b: Operasi seleksi dapat digabungkan dengan theta join.
class SelectionThetaJoinRule(OptimizationRule):
    @property
    def name(self) -> str:
        return "SelectionThetaJoin"
    
    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != QueryNodeType.SELECTION:
            return False

        if not node.children or len(node.children) == 0:
            return False

        child = node.children[0]
        return child.type in [QueryNodeType.THETA_JOIN, QueryNodeType.JOIN]
    
    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        
        join_node = node.children[0]
        
        # Combine conditions
        join_condition = join_node.value if join_node.value else ""
        selection_condition = node.value if node.value else ""
        
        # Create combined condition
        if join_condition and selection_condition:
            combined_condition = f"{join_condition} AND {selection_condition}"
        elif selection_condition:
            combined_condition = selection_condition
        else:
            combined_condition = join_condition
        
        new_join = QueryTree(
            type=join_node.type,
            value=combined_condition,
            children=join_node.children.copy(),
            parent=None
        )
        
        # Update parent references
        for child in new_join.children:
            child.parent = new_join
        
        return new_join