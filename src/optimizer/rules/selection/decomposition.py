from typing import Optional, List
from src.optimizer.rules.base_rule import OptimizationRule
from src.core.models.query import QueryTree, QueryNodeType

# Rule 1: Operasi seleksi konjungtif dapat diuraikan menjadi urutan seleksi.
class SelectionDecompositionRule(OptimizationRule):
    @property
    def name(self) -> str:
        return "SelectionDecomposition"
    
    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != QueryNodeType.SELECTION:
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
                type=QueryNodeType.SELECTION,
                value=condition.strip(),
                children=[current_tree],
                parent=None
            )
            current_tree.parent = new_selection
            current_tree = new_selection
        
        return current_tree
    
    def _split_and_conditions(self, condition: str) -> List[str]:
        # Split condition by AND operator while respecting parentheses
        import re

        # Find all AND positions (case insensitive)
        and_pattern = re.compile(r'\s+AND\s+', re.IGNORECASE)
        matches = list(and_pattern.finditer(condition))

        if not matches:
            return [condition]

        # Track parentheses depth and find valid split positions
        valid_splits = []
        for match in matches:
            pos = match.start()
            # Check parentheses balance up to this position
            depth = 0
            for i in range(pos):
                if condition[i] == '(':
                    depth += 1
                elif condition[i] == ')':
                    depth -= 1

            # Only split at top-level ANDs (depth == 0)
            if depth == 0:
                valid_splits.append((match.start(), match.end()))

        if not valid_splits:
            return [condition]

        # Split the condition at valid positions
        parts = []
        start = 0
        for split_start, split_end in valid_splits:
            parts.append(condition[start:split_start].strip())
            start = split_end
        parts.append(condition[start:].strip())

        return [part for part in parts if part]