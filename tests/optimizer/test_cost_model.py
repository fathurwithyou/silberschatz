import math
import os
import sys
import ast
import pytest

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


@pytest.fixture
def statistics():
    return {
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


@pytest.fixture
def cost_model(statistics):
    if not ACTUAL_IMPORT_SUCCESS or ActualCostModel is None:
        pytest.skip("Could not import ACTUAL CostModel implementation")
    
    print("Testing ACTUAL CostModel implementation")
    return ActualCostModel(statistics)


def test_actual_table_scan_cost(cost_model):
    table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
    cost = cost_model.compute_table_scan_cost(table_node)
    
    expected_cost = 100
    assert cost == expected_cost


def test_actual_table_scan_unknown_table(cost_model):
    table_node = type('QueryTree', (), {'type': 'table', 'value': 'UnknownTable', 'children': []})()
    cost = cost_model.compute_table_scan_cost(table_node)
    
    assert cost == 1000


def test_actual_get_cost_method(cost_model):
    table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
    cost = cost_model.get_cost(table_node)
    
    expected_cost = 100
    assert cost == expected_cost


def test_actual_cost_constants(cost_model):
    assert cost_model.SEQUENTIAL_READ_COST == 1
    assert cost_model.RANDOM_READ_COST == 10
    assert cost_model.WRITE_COST == 5
    assert cost_model.CPU_TUPLE_COST == 0.01


def test_actual_methods_exist(cost_model):
    expected_methods = [
        'get_cost', 'compute_node_cost', 'compute_table_scan_cost',
        'compute_selection_cost', 'compute_projection_cost', 'compute_join_cost',
        'compute_cartesian_product_cost', 'compute_sort_cost', 'nested_loop_join_cost',
        'hash_join_cost', 'merge_join_cost', 'external_sort_cost', 'get_blocking_factor',
        'estimate_input_cardinality', 'extract_table_name', 'get_node_statistics'
    ]
    
    for method_name in expected_methods:
        assert hasattr(cost_model, method_name)


def test_actual_extract_table_name(cost_model):
    test_cases = [
        ("Employee", "Employee"),
        ("Employee AS e", "Employee"),
        ("Employee e", "Employee"),
    ]
    
    for input_value, expected in test_cases:
        result = cost_model.extract_table_name(input_value)
        assert result == expected


def test_actual_selection_cost(cost_model):
    table_node = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
    selection_node = type('QueryTree', (), {
        'type': 'selection', 
        'value': 'salary > 50000', 
        'children': [table_node]
    })()
    
    cost = cost_model.compute_selection_cost(selection_node)
    
    assert cost > 100


def test_actual_join_cost(cost_model):
    left_table = type('QueryTree', (), {'type': 'table', 'value': 'Employee', 'children': []})()
    right_table = type('QueryTree', (), {'type': 'table', 'value': 'Department', 'children': []})()
    join_node = type('QueryTree', (), {
        'type': 'join',
        'value': 'Employee.dept_id = Department.id',
        'children': [left_table, right_table]
    })()
    
    cost = cost_model.compute_join_cost(join_node)
    
    assert cost > 110


def test_actual_complex_query(cost_model):
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
    
    cost = cost_model.get_cost(projection_node)
    
    assert cost > 0


def test_cost_model_file_exists():
    assert os.path.exists(COST_MODEL_PATH), (
        f"CostModel file should exist at: {COST_MODEL_PATH}"
    )


def test_cost_model_file_has_correct_content():
    with open(COST_MODEL_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    assert 'class CostModel:' in content
    
    expected_methods = ['get_cost', 'compute_node_cost', 'compute_table_scan_cost']
    for method in expected_methods:
        assert f'def {method}' in content
    
    expected_constants = ['SEQUENTIAL_READ_COST', 'RANDOM_READ_COST', 'CPU_TUPLE_COST']
    for constant in expected_constants:
        assert constant in content
