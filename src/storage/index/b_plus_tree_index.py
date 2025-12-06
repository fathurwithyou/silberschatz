# Function for B+ Tree Implementation
from typing import Any, List, Optional
from .base_index import BaseIndex


class BPlusTreeNode:

    def __init__(self, order: int, is_leaf: bool = False):
        self.order = order
        self.is_leaf = is_leaf
        self.keys = []
        self.children = [] 
        self.next = None

    def is_full(self) -> bool:
        return len(self.keys) >= self.order - 1

    def find_index(self, key: Any) -> int:
        for i, k in enumerate(self.keys):
            if key < k:
                return i
        return len(self.keys)


class BPlusTreeIndex(BaseIndex):

    def __init__(self, table_name: str, column_name: str, data_directory: str = "data", order: int = 4):
        super().__init__(table_name, column_name, data_directory)
        self.order = order 
        self.root = BPlusTreeNode(order, is_leaf=True)
        self.load() 

    def get_index_type(self) -> str:
        return "b_plus_tree"

    def insert(self, key: Any, row_id: int) -> None:
        """
        Masukkan:
            key: Nilai dari kolom yang di-index
            row_id: Row ID dalam table
        """
        if self.root.is_full():
            old_root = self.root
            self.root = BPlusTreeNode(self.order, is_leaf=False)
            self.root.children.append(old_root)
            self._split_child(self.root, 0)

        self._insert_non_full(self.root, key, row_id)
        self.save()

    def _insert_non_full(self, node: BPlusTreeNode, key: Any, row_id: int) -> None:
        if node.is_leaf:

            for i, k in enumerate(node.keys):
                if k == key:
                    if isinstance(node.children[i], list):
                        node.children[i].append(row_id)
                    else:
                        node.children[i] = [node.children[i], row_id]
                    return

            index = node.find_index(key)
            node.keys.insert(index, key)
            node.children.insert(index, [row_id])
        else:
            index = node.find_index(key)
            child = node.children[index]

            if child.is_full():
                self._split_child(node, index)
                if key > node.keys[index]:
                    index += 1

            self._insert_non_full(node.children[index], key, row_id)

    def _split_child(self, parent: BPlusTreeNode, index: int) -> None:
        full_child = parent.children[index]
        mid = len(full_child.keys) // 2

        new_child = BPlusTreeNode(self.order, is_leaf=full_child.is_leaf)

        if full_child.is_leaf:
            new_child.keys = full_child.keys[mid:]
            new_child.children = full_child.children[mid:]
            full_child.keys = full_child.keys[:mid]
            full_child.children = full_child.children[:mid]

            new_child.next = full_child.next
            full_child.next = new_child

            parent.keys.insert(index, new_child.keys[0])
        else:
            parent.keys.insert(index, full_child.keys[mid])
            new_child.keys = full_child.keys[mid + 1:]
            new_child.children = full_child.children[mid + 1:]
            full_child.keys = full_child.keys[:mid]
            full_child.children = full_child.children[:mid + 1]

        parent.children.insert(index + 1, new_child)

    def search(self, key: Any) -> List[int]:
        """
        Search untuk exact key

        Masukkan:
            key: Nilai yang dicari

        Returns:
            List of row IDs
        """
        return self._search_recursive(self.root, key)

    def _search_recursive(self, node: BPlusTreeNode, key: Any) -> List[int]:
        """Recursive search"""
        if node.is_leaf:
            for i, k in enumerate(node.keys):
                if k == key:
                    return node.children[i] if isinstance(node.children[i], list) else [node.children[i]]
            return []
        else:
            index = node.find_index(key)
            return self._search_recursive(node.children[index], key)

    def range_search(self, start_key: Any, end_key: Any) -> List[int]:
        """
        Masukkan:
            start_key: Start of range (inclusive)
            end_key: End of range (inclusive)

        Returns:
            List of row IDs dalam range
        """
        result = []

        leaf = self._find_leaf(self.root, start_key)

        while leaf:
            for i, key in enumerate(leaf.keys):
                if start_key <= key <= end_key:
                    row_ids = leaf.children[i]
                    if isinstance(row_ids, list):
                        result.extend(row_ids)
                    else:
                        result.append(row_ids)
                elif key > end_key:
                    return result

            leaf = leaf.next

        return result

    def range_search_greater_than(self, start_key: Any, inclusive: bool = True) -> List[int]:
        """
        Range search untuk x >= start_key atau x > start_key

        Masukkan:
            start_key: Batas bawah
            inclusive: True untuk >=, False untuk >

        Returns:
            List of row IDs yang memenuhi kondisi
        """
        result = []

        leaf = self._find_leaf(self.root, start_key)

        while leaf:
            for i, key in enumerate(leaf.keys):
                if inclusive:
                    if key >= start_key:
                        row_ids = leaf.children[i]
                        if isinstance(row_ids, list):
                            result.extend(row_ids)
                        else:
                            result.append(row_ids)
                else:
                    if key > start_key:
                        row_ids = leaf.children[i]
                        if isinstance(row_ids, list):
                            result.extend(row_ids)
                        else:
                            result.append(row_ids)

            leaf = leaf.next

        return result

    def range_search_less_than(self, end_key: Any, inclusive: bool = True) -> List[int]:
        """
        Range search untuk x <= end_key atau x < end_key

        Masukkan:
            end_key: Batas atas
            inclusive: True untuk <=, False untuk <

        Returns:
            List of row IDs yang memenuhi kondisi
        """
        result = []

        leaf = self._find_leftmost_leaf(self.root)

        while leaf:
            for i, key in enumerate(leaf.keys):
                if inclusive:
                    if key <= end_key:
                        row_ids = leaf.children[i]
                        if isinstance(row_ids, list):
                            result.extend(row_ids)
                        else:
                            result.append(row_ids)
                    else:
                        return result
                else:
                    if key < end_key:
                        row_ids = leaf.children[i]
                        if isinstance(row_ids, list):
                            result.extend(row_ids)
                        else:
                            result.append(row_ids)
                    else:
                        return result

            leaf = leaf.next

        return result

    def range_search_advanced(self, start_key: Any = None, end_key: Any = None,
                             start_inclusive: bool = True, end_inclusive: bool = True) -> List[int]:
        """
        Range search yang fleksibel dengan support untuk:
        - Bounded range: start_key <= x <= end_key
        - Unbounded bawah: x <= end_key (start_key = None)
        - Unbounded atas: x >= start_key (end_key = None)
        - Strict inequalities: menggunakan start_inclusive dan end_inclusive

        Masukkan:
            start_key: Batas bawah (None untuk unbounded)
            end_key: Batas atas (None untuk unbounded)
            start_inclusive: True untuk >=, False untuk >
            end_inclusive: True untuk <=, False untuk <

        Returns:
            List of row IDs yang memenuhi kondisi
        """
        if start_key is None and end_key is None:
            result = []
            leaf = self._find_leftmost_leaf(self.root)
            while leaf:
                for i in range(len(leaf.keys)):
                    row_ids = leaf.children[i]
                    if isinstance(row_ids, list):
                        result.extend(row_ids)
                    else:
                        result.append(row_ids)
                leaf = leaf.next
            return result

        if start_key is None:
            return self.range_search_less_than(end_key, end_inclusive)

        if end_key is None:
            return self.range_search_greater_than(start_key, start_inclusive)

        result = []
        leaf = self._find_leaf(self.root, start_key)

        while leaf:
            for i, key in enumerate(leaf.keys):
                start_ok = False
                if start_inclusive:
                    start_ok = key >= start_key
                else:
                    start_ok = key > start_key

                end_ok = False
                if end_inclusive:
                    end_ok = key <= end_key
                else:
                    end_ok = key < end_key

                if start_ok and end_ok:
                    row_ids = leaf.children[i]
                    if isinstance(row_ids, list):
                        result.extend(row_ids)
                    else:
                        result.append(row_ids)
                elif key > end_key:
                    return result

            leaf = leaf.next

        return result

    def _find_leaf(self, node: BPlusTreeNode, key: Any) -> Optional[BPlusTreeNode]:
        if node.is_leaf:
            return node

        index = node.find_index(key)
        return self._find_leaf(node.children[index], key)

    def _find_leftmost_leaf(self, node: BPlusTreeNode) -> Optional[BPlusTreeNode]:
        """Helper untuk menemukan leaf paling kiri (smallest keys)"""
        if node.is_leaf:
            return node
        return self._find_leftmost_leaf(node.children[0])

    def delete(self, key: Any, row_id: int) -> None:
        """
        Masukkan:
            key: Key yang akan dihapus
            row_id: Row ID spesifik yang akan dihapus
        """
        self._delete_recursive(self.root, key, row_id)
        self.save()

    def _delete_recursive(self, node: BPlusTreeNode, key: Any, row_id: int) -> None:
        if node.is_leaf:
            for i, k in enumerate(node.keys):
                if k == key:
                    row_ids = node.children[i]
                    if not isinstance(row_ids, list):
                        row_ids = [row_ids]
                    if row_id in row_ids:
                        row_ids.remove(row_id)
                        if not row_ids:
                            node.keys.pop(i)
                            node.children.pop(i)
                        else:
                            node.children[i] = row_ids
                    return
        else:
            index = node.find_index(key)
            if index < len(node.children):
                self._delete_recursive(node.children[index], key, row_id)

    def _get_state(self) -> dict:
        """Return state untuk serialisasi"""
        return {
            'order': self.order,
            'root': self.root,
            'table_name': self.table_name,
            'column_name': self.column_name
        }

    def _set_state(self, state: dict) -> None:
        """Restore state dari serialisasi"""
        self.order = state['order']
        self.root = state['root']
        self.table_name = state['table_name']
        self.column_name = state['column_name']

    def print_tree(self, node: Optional[BPlusTreeNode] = None, level: int = 0) -> None:
        if node is None:
            node = self.root

        print("  " * level + f"Level {level}: Keys={node.keys}, IsLeaf={node.is_leaf}")
        if not node.is_leaf:
            for child in node.children:
                self.print_tree(child, level + 1)
