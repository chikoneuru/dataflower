from typing import Dict, List, Literal, Tuple
from dataclasses import dataclass

@dataclass
class DAGCosts:
    total_compute_cost: float
    total_transfer_cost: float
    total_cold_start_cost: float
    total_interference_cost: float

class ModeClassifier:
    def __init__(self, threshold: float = 1.0):
        self.threshold = threshold  # CCR threshold for classification
    
    def classify_execution_mode(self, dag_costs: DAGCosts) -> Literal["network-bound", "compute-bound"]:
        """
        Classify execution mode based on Communication-to-Computation Ratio (CCR)
        """
        # Calculate CCR
        if dag_costs.total_compute_cost == 0:
            return "network-bound"  # Avoid division by zero
        
        ccr = dag_costs.total_transfer_cost / dag_costs.total_compute_cost
        
        # Classification logic
        if ccr > self.threshold:
            return "network-bound"
        else:
            return "compute-bound"
    
    def get_mode_weights(self, mode: str) -> Dict[str, float]:
        """
        Get weight factors for the chosen mode
        """
        if mode == "network-bound":
            return {
                'alpha': 0.1,   # Low weight on compute
                'beta': 0.7,    # High weight on transfer
                'gamma': 0.1,   # Low weight on cold start
                'delta': 0.1    # Low weight on interference
            }
        else:  # compute-bound
            return {
                'alpha': 0.6,   # High weight on compute
                'beta': 0.1,    # Low weight on transfer
                'gamma': 0.1,   # Low weight on cold start
                'delta': 0.2    # Medium weight on interference
            }
    
    def analyze_dag(self, tasks: List, nodes: List, cost_models) -> Dict:
        """
        Analyze entire DAG and return classification with weights
        """
        # Calculate total costs across DAG
        total_costs = DAGCosts(0, 0, 0, 0)
        
        for task in tasks:
            for node in nodes:
                costs = cost_models.get_all_costs(task, node, [])
                total_costs.total_compute_cost += costs['compute']
                total_costs.total_transfer_cost += costs['transfer']
                total_costs.total_cold_start_cost += costs['cold_start']
                total_costs.total_interference_cost += costs['interference']
        
        # Classify mode
        mode = self.classify_execution_mode(total_costs)
        weights = self.get_mode_weights(mode)
        
        return {
            'mode': mode,
            'weights': weights,
            'ccr': total_costs.total_transfer_cost / max(total_costs.total_compute_cost, 0.001),
            'total_costs': total_costs
        }

    # ===== MOCK FUNCTIONS FOR TESTING =====
    
    def mock_classify_execution_mode(self, dag_costs: DAGCosts) -> Literal["network-bound", "compute-bound"]:
        """
        Mock function: Simple classification based on transfer vs compute ratio
        """
        if dag_costs.total_compute_cost == 0:
            return "network-bound"
        
        ccr = dag_costs.total_transfer_cost / dag_costs.total_compute_cost
        return "network-bound" if ccr > 1.0 else "compute-bound"
    
    def mock_analyze_dag(self, tasks: List, nodes: List, cost_models) -> Dict:
        """
        Mock function: Simple DAG analysis for testing
        """
        # Simple mock calculation
        total_compute = len(tasks) * 1.0  # 1 second per task
        total_transfer = len(tasks) * 0.5  # 0.5 seconds per task
        
        mock_costs = DAGCosts(
            total_compute_cost=total_compute,
            total_transfer_cost=total_transfer,
            total_cold_start_cost=len(tasks) * 0.1,
            total_interference_cost=len(tasks) * 0.2
        )
        
        mode = self.mock_classify_execution_mode(mock_costs)
        weights = self.get_mode_weights(mode)
        
        return {
            'mode': mode,
            'weights': weights,
            'ccr': total_transfer / max(total_compute, 0.001),
            'total_costs': mock_costs
        }
    
    @staticmethod
    def create_mock_dag_costs(compute_heavy: bool = False) -> DAGCosts:
        """
        Create mock DAG costs for testing
        """
        if compute_heavy:
            return DAGCosts(
                total_compute_cost=10.0,
                total_transfer_cost=2.0,
                total_cold_start_cost=1.0,
                total_interference_cost=0.5
            )
        else:
            return DAGCosts(
                total_compute_cost=2.0,
                total_transfer_cost=10.0,
                total_cold_start_cost=1.0,
                total_interference_cost=0.5
            )