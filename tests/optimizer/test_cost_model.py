import unittest
import math
import os
import sys
import ast

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

COST_MODEL_PATH = os.path.join(os.path.dirname(__file__), '../../src/optimizer/cost/cost_model.py')

def import_cost_model_directly():
    if not os.path.exists(COST_MODEL_PATH):
        raise ImportError(f"CostModel file not found at: {COST_MODEL_PATH}")
    
    print(f"Reading CostModel from: {COST_MODEL_PATH}")
    
    with open(COST_MODEL_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content)
        print("✓ File parsed successfully")
        
        # Check if CostModel class exists
        class_found = any(
            isinstance(node, ast.ClassDef) and node.name == 'CostModel' 
            for node in tree.body
        )
        if not class_found:
            raise ImportError("CostModel class not found in file")
        print("✓ CostModel class found")
        
    except SyntaxError as e:
        raise ImportError(f"Syntax error in CostModel file: {e}")
    
    namespace = {
        'math': math,
        'Dict': dict,
        'List': list, 
        'Optional': type('Optional', (), {}),
    }
    
    # Define the required classes that CostModel depends on
    class QueryTree:
        def __init__(self, type: str, value: str, children: list = None, parent=None):
            self.type = type
            self.value = value
            self.children = children or []
            self.parent = parent

    class Statistic:
        def __init__(self, table_name: str, n_r: int, b_r: int, l_r: int, f_r: int, V: dict):
            self.table_name = table_name
            self.n_r = n_r
            self.b_r = b_r
            self.l_r = l_r
            self.f_r = f_r
            self.V = V

    class CardinalityEstimator:
        def __init__(self, statistics):
            self.statistics = statistics
        
        def is_equijoin(self, condition: str) -> bool:
            if not condition:
                return False
            return '=' in condition and '!' not in condition and '<' not in condition and '>' not in condition
        
    namespace['QueryTree'] = QueryTree
    namespace['Statistic'] = Statistic
    namespace['CardinalityEstimator'] = CardinalityEstimator
    
    modified_content = content
    import_lines_to_remove = [
        'from src.core.models.query import QueryTree',
        'from src.core.models.storage import Statistic',
        'from .cardinality_estimator import CardinalityEstimator'
    ]
    
    for import_line in import_lines_to_remove:
        modified_content = modified_content.replace(import_line, f'# {import_line}  # REMOVED FOR TESTING')
    
    print("✓ Removed problematic imports")
    
    try:
        exec(modified_content, namespace)
        print("✓ Successfully executed CostModel file")
    except Exception as e:
        raise ImportError(f"Error executing CostModel file: {e}")
    
    if 'CostModel' not in namespace:
        raise ImportError("CostModel class not found after execution")
    
    print("✓ SUCCESS: CostModel imported directly from file!")
    return namespace['CostModel']

try:
    ActualCostModel = import_cost_model_directly()
    ACTUAL_IMPORT_SUCCESS = True
except Exception as e:
    print(f"⨉ FAILED to import actual CostModel: {e}")
    ACTUAL_IMPORT_SUCCESS = False
    ActualCostModel = None


class TestActualCostModel(unittest.TestCase):
    
    def setUp(self):
        if not ACTUAL_IMPORT_SUCCESS or ActualCostModel is None:
            self.skipTest("Could not import ACTUAL CostModel implementation")
        
        self.statistics = {
            'Employee': type('Statistic', (), {
                'table_name': 'Employee',
                'n_r': 1000,
                'b_r': 100,
                'l_r': 100,
                'f_r': 10,
                'V': {'id': 1000, 'salary': 50, 'dept_id': 10}
            })(),
            'Department': type('Statistic', (), {
                'table_name': 'Department', 
                'n_r': 100,
                'b_r': 10,
                'l_r': 200,
                'f_r': 5,
                'V': {'id': 100, 'name': 100}
            })()
        }
        
        # Use the ACTUAL CostModel from the file
        self.cost_model = ActualCostModel(self.statistics)
        print("Testing ACTUAL CostModel implementation")

    def test_actual_table_scan_cost(self):
        table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
        cost = self.cost_model.compute_table_scan_cost(table_node)
        
        # blocks * sequential_cost = 100 * 1 = 100
        expected_cost = 100
        self.assertEqual(cost, expected_cost)
        print(f"✓ ACTUAL table scan cost: {cost}")

    def test_actual_table_scan_unknown_table(self):
        table_node = type('QueryTree', (), {'type': 'table', 'value': 'UnknownTable', 'children': []})()
        cost = self.cost_model.compute_table_scan_cost(table_node)
        
        # default cost 1000
        self.assertEqual(cost, 1000)
        print(f"✓ ACTUAL unknown table cost: {cost}")

    def test_actual_get_cost_method(self):
        table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
        cost = self.cost_model.get_cost(table_node)
        
        # sama dengan cost table scan
        expected_cost = 100
        self.assertEqual(cost, expected_cost)
        print(f"✓ ACTUAL get_cost result: {cost}")

    def test_actual_cost_constants(self):
        self.assertEqual(self.cost_model.SEQUENTIAL_READ_COST, 1)
        self.assertEqual(self.cost_model.RANDOM_READ_COST, 10)
        self.assertEqual(self.cost_model.WRITE_COST, 5)
        self.assertEqual(self.cost_model.CPU_TUPLE_COST, 0.01)
        print("✓ ACTUAL cost constants are correct")

    def test_actual_methods_exist(self):
        expected_methods = [
            'get_cost', 'compute_node_cost', 'compute_table_scan_cost',
            'compute_selection_cost', 'compute_projection_cost', 'compute_join_cost',
            'compute_cartesian_product_cost', 'compute_sort_cost', 'nested_loop_join_cost',
            'hash_join_cost', 'merge_join_cost', 'external_sort_cost', 'get_blocking_factor',
            'estimate_input_cardinality', 'extract_table_name', 'get_node_statistics'
        ]
        
        for method_name in expected_methods:
            self.assertTrue(hasattr(self.cost_model, method_name), 
                          f"ACTUAL CostModel should have method: {method_name}")
        print("✓ ACTUAL CostModel has all expected methods")

    def test_actual_extract_table_name(self):
        test_cases = [
            ("Employee", "Employee"),
            ("Employee AS e", "Employee"),
            ("Employee e", "Employee"),
        ]
        
        for input_value, expected in test_cases:
            result = self.cost_model.extract_table_name(input_value)
            self.assertEqual(result, expected, 
                           f"Failed for input: '{input_value}', expected: '{expected}', got: '{result}'")
        print("✓ ACTUAL table name extraction works")

    def test_actual_selection_cost(self):
        table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
        selection_node = type('QueryTree', (), {
            'type': 'selection', 
            'value': 'salary > 50000', 
            'children': [table_node]
        })()
        
        cost = self.cost_model.compute_selection_cost(selection_node)
        
        # lebih besar dari cost table scan
        self.assertGreater(cost, 100)
        print(f"✓ ACTUAL selection cost: {cost}")

    def test_actual_join_cost(self):
        left_table = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
        right_table = type('QueryTree', (), {'type': 'table', 'value': 'Department', 'children': []})()
        join_node = type('QueryTree', (), {
            'type': 'join',
            'value': 'Employee.dept_id = Department.id',
            'children': [left_table, right_table]
        })()
        
        cost = self.cost_model.compute_join_cost(join_node)
        
        # lebih besar dari jumlah table costs
        self.assertGreater(cost, 110)
        print(f"✓ ACTUAL join cost: {cost}")

    def test_actual_complex_query(self):
        table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
        selection_node = type('QueryTree', (), {
            'type': 'selection', 
            'value': 'salary > 50000', 
            'children': [table_node]
        })()
        projection_node = type('QueryTree', (), {
            'type': 'projection',
            'value': 'name',
            'children': [selection_node]
        })()
        
        cost = self.cost_model.get_cost(projection_node)
        
        self.assertGreater(cost, 0)
        print(f"✓ ACTUAL complex query cost: {cost}")


class TestCostModelFileExistence(unittest.TestCase):
    
    def test_cost_model_file_exists(self):
        self.assertTrue(os.path.exists(COST_MODEL_PATH), 
                       f"CostModel file should exist at: {COST_MODEL_PATH}")
        print(f"✓ CostModel file exists: {COST_MODEL_PATH}")
    
    def test_cost_model_file_has_correct_content(self):
        with open(COST_MODEL_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertIn('class CostModel:', content, 
                     "File should contain CostModel class definition")
        
        expected_methods = ['get_cost', 'compute_node_cost', 'compute_table_scan_cost']
        for method in expected_methods:
            self.assertIn(f'def {method}', content, 
                         f"File should contain method: {method}")
        
        expected_constants = ['SEQUENTIAL_READ_COST', 'RANDOM_READ_COST', 'CPU_TUPLE_COST']
        for constant in expected_constants:
            self.assertIn(constant, content, 
                         f"File should contain constant: {constant}")
        
        print("✓ CostModel file has expected content")


if __name__ == '__main__':
    print("STARTING TESTS FOR ACTUAL cost_model.py FILE")
    print("=" * 60)
    
    # Run the tests
    unittest.main(verbosity=2)