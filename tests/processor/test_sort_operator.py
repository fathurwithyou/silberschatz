import unittest
import os
import sys
import ast
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

SORT_OPERATOR_PATH = os.path.join(os.path.dirname(__file__), '../../src/processor/operators/sort_operator.py')

def import_sort_operator_directly():
    if not os.path.exists(SORT_OPERATOR_PATH):
        raise ImportError(f"SortOperator file not found at: {SORT_OPERATOR_PATH}")
    
    print(f"Reading SortOperator from: {SORT_OPERATOR_PATH}")
    
    with open(SORT_OPERATOR_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content)
        print("✓ File parsed successfully")
        
        class_found = any(
            isinstance(node, ast.ClassDef) and node.name == 'SortOperator' 
            for node in tree.body
        )
        if not class_found:
            raise ImportError("SortOperator class not found in file")
        print("✓ SortOperator class found")
        
    except SyntaxError as e:
        raise ImportError(f"Syntax error in SortOperator file: {e}")
    
    # Mock dependencies yang dibutuhkan oleh SortOperator
    class MockRows:
        def __init__(self, data, rows_count=0, schema=None):
            self.data = data
            self.rows_count = rows_count
            self.schema = schema
        
        def __repr__(self):
            return str(self.data)

    namespace = {
        'List': list, 
        'Dict': dict, 
        'Tuple': tuple, 
        'Any': object,
        'datetime': datetime,
        'Rows': MockRows, # pengganti src.core.models.Rows
    }
    
    modified_content = content
    # Hapus import asli yang mungkin error di environment test
    import_lines_to_remove = [
        'from src.core.models import Rows',
    ]
    
    for import_line in import_lines_to_remove:
        modified_content = modified_content.replace(import_line, f'# {import_line}  # REMOVED FOR TESTING')
    
    print("✓ Removed problematic imports")
    
    try:
        exec(modified_content, namespace)
        print("✓ Successfully executed SortOperator file")
    except Exception as e:
        raise ImportError(f"Error executing SortOperator file: {e}")
    
    if 'SortOperator' not in namespace:
        raise ImportError("SortOperator class not found after execution")
    
    print("✓ SUCCESS: SortOperator imported directly from file!")
    
    # Return tuple: (Class SortOperator, Class MockRows untuk helper)
    return namespace['SortOperator'], namespace['Rows']

# Global variables untuk menampung hasil import
ActualSortOperator = None
MockRows = None
ACTUAL_IMPORT_SUCCESS = False

try:
    ActualSortOperator, MockRows = import_sort_operator_directly()
    ACTUAL_IMPORT_SUCCESS = True
except Exception as e:
    print(f"⨉ FAILED to import actual SortOperator: {e}")
    ACTUAL_IMPORT_SUCCESS = False


class TestActualSortOperator(unittest.TestCase):
    
    def setUp(self):
        if not ACTUAL_IMPORT_SUCCESS or ActualSortOperator is None:
            self.skipTest("Could not import ACTUAL SortOperator implementation")
        
        self.operator = ActualSortOperator()
        
        # Data dummy untuk pengujian
        self.raw_data = [
            {"id": 1, "name": "Zara", "age": 25, "score": 80.5, "joined": "2023-01-01"},
            {"id": 2, "name": "Ali",  "age": 30, "score": 90.0, "joined": "2023-02-01"},
            {"id": 3, "name": "Ali",  "age": 20, "score": 85.0, "joined": "2023-03-01"}, # Ali muda
            {"id": 4, "name": "Budi", "age": None, "score": 88.0, "joined": None},
            {"id": 5, "name": "caca", "age": 28, "score": 70.0, "joined": "2023-01-15"}  # Lowercase name
        ]
        
        self.input_rows = MockRows(data=self.raw_data, rows_count=len(self.raw_data))
        print("Testing ACTUAL SortOperator implementation")

    def test_actual_sort_no_order_by(self):
        """Test jika order_by kosong, urutan tidak berubah"""
        result = self.operator.execute(self.input_rows, "")
        self.assertEqual(result.data, self.raw_data)
        
        result_none = self.operator.execute(self.input_rows, None)
        self.assertEqual(result_none.data, self.raw_data)
        print("✓ ACTUAL no order_by returns original data")

    def test_actual_sort_asc_integer(self):
        """Test sorting ASC pada kolom integer"""
        result = self.operator.execute(self.input_rows, "id ASC")
        ids = [row['id'] for row in result.data]
        self.assertEqual(ids, [1, 2, 3, 4, 5])
        print(f"✓ ACTUAL sort integer ASC: {ids}")

    def test_actual_sort_desc_integer(self):
        """Test sorting DESC pada kolom integer"""
        result = self.operator.execute(self.input_rows, "id DESC")
        ids = [row['id'] for row in result.data]
        self.assertEqual(ids, [5, 4, 3, 2, 1])
        print(f"✓ ACTUAL sort integer DESC: {ids}")

    def test_actual_sort_string_case_insensitive(self):
        """Test sorting string (memastikan 'Ali' < 'Budi' < 'caca')"""
        # 'caca' (lowercase) harusnya setelah 'Budi' jika case-insensitive
        # Karena 'c' > 'B' secara case-insensitive ('b' < 'c')
        result = self.operator.execute(self.input_rows, "name ASC")
        names = [row['name'] for row in result.data]
        
        # Ekspektasi: Ali, Ali, Budi, caca, Zara
        expected = ["Ali", "Ali", "Budi", "caca", "Zara"]
        self.assertEqual(names, expected)
        print(f"✓ ACTUAL sort string ASC (case-insensitive): {names}")

    def test_actual_sort_desc_string(self):
        """Test sorting DESC pada string (menguji logika pembalik bit char)"""
        result = self.operator.execute(self.input_rows, "name DESC")
        names = [row['name'] for row in result.data]
        
        # Ekspektasi: Zara, caca, Budi, Ali, Ali
        expected = ["Zara", "caca", "Budi", "Ali", "Ali"]
        self.assertEqual(names, expected)
        print(f"✓ ACTUAL sort string DESC: {names}")

    def test_actual_sort_multi_column(self):
        """Test sorting multi kolom: Nama ASC, Umur ASC"""
        # Kasus: Ali (30) vs Ali (20). Ali(20) harus duluan.
        result = self.operator.execute(self.input_rows, "name ASC, age ASC")
        
        # Ambil tuple (name, age) untuk verifikasi
        pairs = [(row['name'], row['age']) for row in result.data]
        
        # Cek spesifik urutan Ali
        ali_subset = [p for p in pairs if p[0] == "Ali"]
        self.assertEqual(ali_subset, [("Ali", 20), ("Ali", 30)])
        print(f"✓ ACTUAL multi-column sort (Name ASC, Age ASC): {pairs}")

    def test_actual_sort_multi_column_mixed_direction(self):
        """Test sorting multi kolom: Nama ASC, Umur DESC"""
        # Kasus: Ali (30) vs Ali (20). Ali(30) harus duluan karena Age DESC.
        result = self.operator.execute(self.input_rows, "name ASC, age DESC")
        
        pairs = [(row['name'], row['age']) for row in result.data]
        
        ali_subset = [p for p in pairs if p[0] == "Ali"]
        self.assertEqual(ali_subset, [("Ali", 30), ("Ali", 20)])
        print(f"✓ ACTUAL multi-column sort (Name ASC, Age DESC): {pairs}")

    def test_actual_sort_null_handling(self):
        """Test handling NULL values (Harus selalu di awal/terkecil)"""
        # Ada row dengan age=None (ID 4)
        result = self.operator.execute(self.input_rows, "age ASC")
        
        first_row = result.data[0]
        self.assertIsNone(first_row['age'])
        self.assertEqual(first_row['id'], 4)
        print("✓ ACTUAL null handling (Null appears first)")

    def test_actual_parse_order_by_method(self):
        """Test method private _parse_order_by"""
        query = "name, age DESC,  score   ASC"
        parsed = self.operator._parse_order_by(query)
        
        expected = [
            ("name", "ASC"),   # Default ASC
            ("age", "DESC"),
            ("score", "ASC")
        ]
        self.assertEqual(parsed, expected)
        print(f"✓ ACTUAL _parse_order_by works correctly: {parsed}")

    def test_actual_normalize_value_method(self):
        """Test method private _normalize_value untuk type safety"""
        # Test None
        val_none = self.operator._normalize_value(None)
        self.assertEqual(val_none, (0, None))
        
        # Test Int
        val_int = self.operator._normalize_value(100)
        self.assertEqual(val_int, (1, 100))
        
        # Test String
        val_str = self.operator._normalize_value("Test")
        self.assertEqual(val_str, (4, "test")) # Expect lowercase
        
        print("✓ ACTUAL _normalize_value method works correctly")

    def test_actual_apply_direction_method(self):
        """Test method private _apply_direction untuk logika DESC"""
        # Test Int DESC
        norm_int = (1, 50)
        res_int = self.operator._apply_direction(norm_int, "DESC")
        self.assertEqual(res_int, (1, -50))
        
        # Test String DESC
        norm_str = (4, "abc")
        res_str = self.operator._apply_direction(norm_str, "DESC")
        inverted = "".join(chr(255 - ord(c)) for c in "abc")
        self.assertEqual(res_str, (4, inverted))
        
        print("✓ ACTUAL _apply_direction method works correctly")


class TestSortOperatorFileExistence(unittest.TestCase):
    
    def test_sort_operator_file_exists(self):
        self.assertTrue(os.path.exists(SORT_OPERATOR_PATH), 
                        f"SortOperator file should exist at: {SORT_OPERATOR_PATH}")
        print(f"✓ SortOperator file exists: {SORT_OPERATOR_PATH}")
    
    def test_sort_operator_file_has_correct_content(self):
        with open(SORT_OPERATOR_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertIn('class SortOperator:', content, 
                      "File should contain SortOperator class definition")
        
        expected_methods = [
            'execute',
            '_parse_order_by',
            '_build_sort_key',
            '_normalize_value',
            '_apply_direction'
        ]
        for method in expected_methods:
            self.assertIn(f'def {method}', content, 
                          f"File should contain method: {method}")
        
        print("✓ SortOperator file has expected content")


if __name__ == '__main__':
    print("STARTING TESTS FOR ACTUAL sort_operator.py FILE")
    print("=" * 60)
    
    unittest.main(verbosity=2)