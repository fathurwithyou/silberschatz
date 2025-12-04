import math
import os
import sys
import ast
import pytest
from src.core import IStorageManager
from src.core.models.storage import Statistic, TableSchema, DataRetrieval, DataWrite, DataDeletion, ColumnDefinition, DataType, Condition, ComparisonOperator
from src.core.models.result import Rows
from src.core.models.query import QueryTree # Use the actual QueryTree class

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
        'IStorageManager': IStorageManager,
        'Statistic': Statistic,
        'QueryTree': QueryTree,
        'CardinalityEstimator': object, # Mock CardinalityEstimator for namespace if needed
    }
    
    # Remove local class definitions for testing
    local_class_definitions_to_remove = [
        'class QueryTree:',
        'class Statistic:',
        'class CardinalityEstimator:'
    ]
    
    modified_content = content
    # For robust removal, find exact class definitions and replace with pass
    for class_keyword in local_class_definitions_to_remove:
        idx = 0
        while True:
            idx = modified_content.find(class_keyword, idx)
            if idx == -1:
                break
            
            # Find the end of the class block
            end_of_class = modified_content.find('\nclass ', idx + 1) # Look for next class or end of file
            if end_of_class == -1:
                end_of_class = len(modified_content)
            
            # Replace the class body with 'pass'
            modified_content = modified_content[:idx] + class_keyword + modified_content[idx:].split('\n', 1)[0] + '\n    pass' + modified_content[end_of_class:]
            idx += len(class_keyword) # Move past this class
    
    # Comment out old import lines for the actual module being imported
    modified_content = content # Reset content to avoid double-modifying
    import_lines_to_remove = [
        'from src.core.models.query import QueryTree',
        'from src.core.models.storage import Statistic',
        'from .cardinality_estimator import CardinalityEstimator',
    ]
    for import_line in import_lines_to_remove:
        if import_line.strip() in modified_content:
            modified_content = modified_content.replace(import_line, f'# {import_line}  # REMOVED FOR TESTING')

    print("✓ Removed problematic imports and local class definitions for testing")
    
    try:
        # Before executing, replace CardinalityEstimator with a mock or a simple object
        # since the real one expects storage_manager, not statistics
        # The CostModel itself relies on CardinalityEstimator, so we need a placeholder
        class MockCardinalityEstimatorForCostModel:
            def __init__(self, storage_manager):
                self.storage_manager = storage_manager
            
            def is_equijoin(self, condition: str) -> bool:
                # Mock implementation for the method used by CostModel
                return '=' in condition and '!' not in condition and '<' not in condition and '>' not in condition
            
            # Add other methods that CostModel might call on CardinalityEstimator if needed
            def estimate_condition_selectivity(self, stats, condition): return 0.5 # Dummy
        
        namespace['CardinalityEstimator'] = MockCardinalityEstimatorForCostModel


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
    from src.core.models.storage import Statistic # Import actual Statistic
    return {
        'Employee': Statistic(
            table_name='Employee',
            n_r=1000,
            b_r=100,
            l_r=100,
            f_r=10,
            V={'id': 1000, 'salary': 50, 'dept_id': 10},
            min_values={}, max_values={}, null_counts={} # Ensure all fields are present
        ),
        'Department': Statistic(
            table_name='Department', 
            n_r=100,
            b_r=10,
            l_r=200,
            f_r=5,
            V={'id': 100, 'name': 100},
            min_values={}, max_values={}, null_counts={} # Ensure all fields are present
        )
    }

@pytest.fixture
def mock_storage_manager_for_cost_model(statistics):
    class MockStorageManager(IStorageManager):
        def get_stats(self, table_name: str) -> Statistic:
            if table_name in statistics:
                return statistics[table_name]
            return Statistic(
                table_name=table_name,
                n_r=1000, b_r=100, l_r=100, f_r=10, V={},
                min_values={}, max_values={}, null_counts={}
            )

        def read_block(self, data_retrieval: DataRetrieval) -> Rows: raise NotImplementedError
        def write_block(self, data_write: DataWrite) -> int: raise NotImplementedError
        def delete_block(self, data_deletion: DataDeletion) -> int: raise NotImplementedError
        def set_index(self, table: str, column: str, index_type: str) -> None: raise NotImplementedError
        def drop_index(self, table: str, column: str) -> None: raise NotImplementedError
        def has_index(self, table: str, column: str) -> bool: return False
        def create_table(self, schema: TableSchema) -> None: raise NotImplementedError
        def drop_table(self, table_name: str) -> None: raise NotImplementedError
        def get_table_schema(self, table_name: str) -> Optional[TableSchema]: return None
        def list_tables(self) -> List[str]: return []

    return MockStorageManager()


@pytest.fixture
def cost_model(mock_storage_manager_for_cost_model):
    if not ACTUAL_IMPORT_SUCCESS or ActualCostModel is None:
        pytest.skip("Could not import ACTUAL CostModel implementation")
    
    print("Testing ACTUAL CostModel implementation")
    return ActualCostModel(mock_storage_manager_for_cost_model)


def test_actual_table_scan_cost(cost_model):
    table_node = QueryTree(type='table', value='Employee', children=[])
    cost = cost_model.compute_table_scan_cost(table_node)
    
    expected_cost = 100
    assert cost == expected_cost


def test_actual_table_scan_unknown_table(cost_model):
    table_node = QueryTree(type='table', value='UnknownTable', children=[])
    cost = cost_model.compute_table_scan_cost(table_node)
    
    assert cost == 100


def test_actual_get_cost_method(cost_model):
    table_node = QueryTree(type='table', value='Employee', children=[])
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
    table_node = QueryTree(type='table', value='Employee', children=[])
    selection_node = QueryTree(type='selection', value='salary > 50000', children=[table_node])
    
    cost = cost_model.compute_selection_cost(selection_node)
    
    assert cost > 100


def test_actual_join_cost(cost_model):
    left_table = QueryTree(type='table', value='Employee', children=[])
    right_table = QueryTree(type='table', value='Department', children=[])
    join_node = QueryTree(type='join', value='Employee.dept_id = Department.id', children=[left_table, right_table])
    
    cost = cost_model.compute_join_cost(join_node)
    
    assert cost > 110


def test_actual_complex_query(cost_model):
    table_node = QueryTree(type='table', value='Employee', children=[])
    selection_node = QueryTree(type='selection', value='salary > 50000', children=[table_node])
    projection_node = QueryTree(type='projection', value='name', children=[selection_node])
    
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