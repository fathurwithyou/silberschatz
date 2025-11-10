import unittest
import os
import sys
import ast

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

CARDINALITY_ESTIMATOR_PATH = os.path.join(os.path.dirname(__file__), '../../src/optimizer/cost/cardinality_estimator.py')

def import_cardinality_estimator_directly():
    if not os.path.exists(CARDINALITY_ESTIMATOR_PATH):
        raise ImportError(f"CardinalityEstimator file not found at: {CARDINALITY_ESTIMATOR_PATH}")
    
    print(f"Reading CardinalityEstimator from: {CARDINALITY_ESTIMATOR_PATH}")
    
    with open(CARDINALITY_ESTIMATOR_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content)
        print("✓ File parsed successfully")
        
        class_found = any(
            isinstance(node, ast.ClassDef) and node.name == 'CardinalityEstimator' 
            for node in tree.body
        )
        if not class_found:
            raise ImportError("CardinalityEstimator class not found in file")
        print("✓ CardinalityEstimator class found")
        
    except SyntaxError as e:
        raise ImportError(f"Syntax error in CardinalityEstimator file: {e}")
    
    namespace = {
        'Dict': dict,
        'List': list, 
        'Optional': type('Optional', (), {}),
        'Set': set,
        'math': __import__('math'),
        're': __import__('re'),
    }
    
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

    class Condition:
        def __init__(self, column: str, operator: str, value):
            self.column = column
            self.operator = operator
            self.value = value

    class ComparisonOperator:
        EQ = "="
        NE = "!="
        LT = "<"
        LE = "<="
        GT = ">"
        GE = ">="

    namespace['QueryTree'] = QueryTree
    namespace['Statistic'] = Statistic
    namespace['Condition'] = Condition
    namespace['ComparisonOperator'] = ComparisonOperator
    
    modified_content = content
    import_lines_to_remove = [
        'from src.core.models.query import QueryTree',
        'from src.core.models.storage import Statistic, Condition, ComparisonOperator'
    ]
    
    for import_line in import_lines_to_remove:
        modified_content = modified_content.replace(import_line, f'# {import_line}  # REMOVED FOR TESTING')
    
    print("✓ Removed problematic imports")
    
    try:
        exec(modified_content, namespace)
        print("✓ Successfully executed CardinalityEstimator file")
    except Exception as e:
        raise ImportError(f"Error executing CardinalityEstimator file: {e}")
    
    if 'CardinalityEstimator' not in namespace:
        raise ImportError("CardinalityEstimator class not found after execution")
    
    print("✓ SUCCESS: CardinalityEstimator imported directly from file!")
    return namespace['CardinalityEstimator']

try:
    ActualCardinalityEstimator = import_cardinality_estimator_directly()
    ACTUAL_IMPORT_SUCCESS = True
except Exception as e:
    print(f"⨉ FAILED to import actual CardinalityEstimator: {e}")
    ACTUAL_IMPORT_SUCCESS = False
    ActualCardinalityEstimator = None


class TestActualCardinalityEstimator(unittest.TestCase):
    
    def setUp(self):
        if not ACTUAL_IMPORT_SUCCESS or ActualCardinalityEstimator is None:
            self.skipTest("Could not import ACTUAL CardinalityEstimator implementation")
        
        self.statistics = {
            'Employee': type('Statistic', (), {
                'table_name': 'Employee',
                'n_r': 1000,
                'b_r': 100,
                'l_r': 100, 
                'f_r': 10,
                'V': {
                    'id': 1000,      # 1000 distinct values (primary key)
                    'salary': 50,    # 50 distinct salary values  
                    'dept_id': 10,   # 10 distinct departments
                    'name': 800      # 800 distinct names
                }
            })(),
            'Department': type('Statistic', (), {
                'table_name': 'Department',
                'n_r': 100,
                'b_r': 10,
                'l_r': 200,
                'f_r': 5,
                'V': {
                    'id': 100,       # 100 distinct values (primary key)
                    'name': 100      # 100 distinct names
                }
            })()
        }
        
        self.estimator = ActualCardinalityEstimator(self.statistics)
        print("Testing ACTUAL CardinalityEstimator implementation")

    def test_actual_selection_cardinality_no_conditions(self):
        cardinality = self.estimator.estimate_selection_cardinality('Employee', [])
        
        expected_cardinality = 1000
        self.assertEqual(cardinality, expected_cardinality)
        print(f"✓ ACTUAL no conditions cardinality: {cardinality}")

    def test_actual_selection_cardinality_unknown_table(self):
        condition = type('Condition', (), {'column': 'salary', 'operator': '=', 'value': 50000})()
        cardinality = self.estimator.estimate_selection_cardinality('UnknownTable', [condition])
        
        # default estimate
        self.assertEqual(cardinality, 1000)
        print(f"✓ ACTUAL unknown table cardinality: {cardinality}")

    def test_actual_selection_cardinality_equality_condition(self):
        condition = type('Condition', (), {'column': 'salary', 'operator': '=', 'value': 50000})()
        cardinality = self.estimator.estimate_selection_cardinality('Employee', [condition])
        
        # base_cardinality * (1/V(A)) = 1000 * (1/50) = 20
        expected_cardinality = 1000 * (1.0 / 50)
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL equality condition cardinality: {cardinality}")

    def test_actual_selection_cardinality_inequality_condition(self):
        condition = type('Condition', (), {'column': 'salary', 'operator': '!=', 'value': 50000})()
        cardinality = self.estimator.estimate_selection_cardinality('Employee', [condition])
        
        # base_cardinality * (1 - 1/V(A)) = 1000 * (1 - 1/50) = 980
        expected_cardinality = 1000 * (1.0 - (1.0 / 50))
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL inequality condition cardinality: {cardinality}")

    def test_actual_selection_cardinality_range_condition(self):
        condition = type('Condition', (), {'column': 'salary', 'operator': '>', 'value': 50000})()
        cardinality = self.estimator.estimate_selection_cardinality('Employee', [condition])
        
        # default range selectivity
        expected_cardinality = 1000 * 0.33
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL range condition cardinality: {cardinality}")

    def test_actual_selection_cardinality_unknown_column(self):
        condition = type('Condition', (), {'column': 'unknown_column', 'operator': '=', 'value': 50000})()
        cardinality = self.estimator.estimate_selection_cardinality('Employee', [condition])
        
        # default selectivity for unknown column
        expected_cardinality = 1000 * 0.1
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL unknown column cardinality: {cardinality}")

    def test_actual_selection_cardinality_multiple_conditions(self):
        conditions = [
            type('Condition', (), {'column': 'salary', 'operator': '>', 'value': 50000})(),
            type('Condition', (), {'column': 'dept_id', 'operator': '=', 'value': 5})()
        ]
        cardinality = self.estimator.estimate_selection_cardinality('Employee', conditions)
        
        # 0.33 * (1/10) = 0.033
        expected_cardinality = 1000 * 0.33 * (1.0 / 10)
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL multiple conditions cardinality: {cardinality}")

    def test_actual_join_cardinality_equijoin(self):
        left_stats = self.statistics['Employee']
        right_stats = self.statistics['Department']
        
        join_condition = 'dept_id = id'
        
        cardinality = self.estimator.estimate_join_cardinality(left_stats, right_stats, join_condition)
        
        # |R| * |S| / max(V(A,R), V(A,S)) = 1000 * 100 / max(10, 100) = 1000
        expected_cardinality = (1000 * 100) / max(10, 100)
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL equijoin cardinality: {cardinality}")

    def test_actual_join_cardinality_theta_join(self):
        left_stats = self.statistics['Employee']
        right_stats = self.statistics['Department']
        join_condition = 'Employee.salary > Department.budget'  # Non-equijoin
        
        cardinality = self.estimator.estimate_join_cardinality(left_stats, right_stats, join_condition)
        
        # |R| * |S| * 0.1 = 1000 * 100 * 0.1 = 10000
        expected_cardinality = 1000 * 100 * 0.1
        self.assertAlmostEqual(cardinality, expected_cardinality, places=2)
        print(f"✓ ACTUAL theta join cardinality: {cardinality}")

    def test_actual_projection_cardinality(self):
        input_cardinality = 500
        cardinality = self.estimator.estimate_projection_cardinality(input_cardinality)
        
        # Projection tidak mengubah cardinality
        self.assertEqual(cardinality, input_cardinality)
        print(f"✓ ACTUAL projection cardinality: {cardinality}")

    def test_actual_cartesian_product_cardinality(self):
        left_card = 1000
        right_card = 100
        cardinality = self.estimator.estimate_cartesian_product_cardinality(left_card, right_card)
        
        expected_cardinality = 1000 * 100
        self.assertEqual(cardinality, expected_cardinality)
        print(f"✓ ACTUAL cartesian product cardinality: {cardinality}")

    def test_actual_is_equijoin_method(self):
        equijoin_conditions = [
            'Employee.dept_id = Department.id',
            'a = b',
            'table1.col1 = table2.col2'
        ]
        for condition in equijoin_conditions:
            self.assertTrue(self.estimator.is_equijoin(condition),
                          f"Should be equijoin: {condition}")
        
        non_equijoin_conditions = [
            'Employee.salary > Department.budget',
            'age != 30',
            'salary <= 50000'
        ]
        for condition in non_equijoin_conditions:
            self.assertFalse(self.estimator.is_equijoin(condition),
                           f"Should not be equijoin: {condition}")
        
        print("✓ ACTUAL is_equijoin method works correctly")

    def test_actual_extract_join_columns_method(self):
        condition = 'Employee.dept_id = Department.id'
        columns = self.estimator.extract_join_columns(condition)
        self.assertIsNotNone(columns)
        self.assertEqual(columns, ('Employee.dept_id', 'Department.id'))
        
        condition = 'dept_id = id'
        columns = self.estimator.extract_join_columns(condition)
        self.assertIsNotNone(columns)
        self.assertEqual(columns, ('dept_id', 'id'))
        
        condition = 'salary > 50000'
        columns = self.estimator.extract_join_columns(condition)
        self.assertIsNone(columns)
        
        print("✓ ACTUAL extract_join_columns method works correctly")

    def test_actual_estimate_condition_selectivity_method(self):
        stats = self.statistics['Employee']
        
        eq_condition = type('Condition', (), {'column': 'salary', 'operator': '=', 'value': 50000})()
        selectivity = self.estimator.estimate_condition_selectivity(stats, eq_condition)
        expected_selectivity = 1.0 / 50  # 1/V(A) where V(A)=50
        self.assertAlmostEqual(selectivity, expected_selectivity, places=4)
        
        range_condition = type('Condition', (), {'column': 'salary', 'operator': '>', 'value': 50000})()
        selectivity = self.estimator.estimate_condition_selectivity(stats, range_condition)
        self.assertAlmostEqual(selectivity, 0.33, places=2)
        
        print("✓ ACTUAL estimate_condition_selectivity method works correctly")

    def test_actual_default_selectivity_method(self):
        test_cases = [
            ('=', 0.1),
            ('!=', 0.9),
            ('>', 0.33),
            ('<', 0.33),
            ('>=', 0.33),
            ('<=', 0.33)
        ]
        
        for operator, expected in test_cases:
            selectivity = self.estimator.default_selectivity(operator)
            self.assertAlmostEqual(selectivity, expected, places=2,
                                 msg=f"Failed for operator: {operator}")
        
        print("✓ ACTUAL default_selectivity method works correctly")

    def test_actual_methods_exist(self):
        expected_methods = [
            'estimate_selection_cardinality',
            'estimate_condition_selectivity',
            'default_selectivity',
            'estimate_join_cardinality', 
            'estimate_projection_cardinality',
            'estimate_cartesian_product_cardinality',
            'is_equijoin',
            'extract_join_columns'
        ]
        
        for method_name in expected_methods:
            self.assertTrue(hasattr(self.estimator, method_name), 
                          f"ACTUAL CardinalityEstimator should have method: {method_name}")
        print("✓ ACTUAL CardinalityEstimator has all expected methods")


class TestCardinalityEstimatorFileExistence(unittest.TestCase):
    
    def test_cardinality_estimator_file_exists(self):
        self.assertTrue(os.path.exists(CARDINALITY_ESTIMATOR_PATH), 
                       f"CardinalityEstimator file should exist at: {CARDINALITY_ESTIMATOR_PATH}")
        print(f"✓ CardinalityEstimator file exists: {CARDINALITY_ESTIMATOR_PATH}")
    
    def test_cardinality_estimator_file_has_correct_content(self):
        with open(CARDINALITY_ESTIMATOR_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertIn('class CardinalityEstimator:', content, 
                     "File should contain CardinalityEstimator class definition")
        
        expected_methods = [
            'estimate_selection_cardinality',
            'estimate_condition_selectivity',
            'estimate_join_cardinality',
            'default_selectivity'
        ]
        for method in expected_methods:
            self.assertIn(f'def {method}', content, 
                         f"File should contain method: {method}")
        
        print("✓ CardinalityEstimator file has expected content")


if __name__ == '__main__':
    print("STARTING TESTS FOR ACTUAL cardinality_estimator.py FILE")
    print("=" * 60)
    
    unittest.main(verbosity=2)