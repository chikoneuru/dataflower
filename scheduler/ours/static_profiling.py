"""
Static Profiling System for Ours Scheduler

Pre-measures execution times and stores them as fixed profiles to eliminate
runtime profiling overhead while maintaining accurate cost predictions.

Strategy:
1. Pre-experiment calibration phase measures function/node performance
2. Profiles stored as static JSON files
3. Cost models use fixed values - no runtime fitting or online delay
"""

import json
import os
import time
import math
import statistics
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger(__name__)

@dataclass
class FunctionProfile:
    """Static profile for a function's execution characteristics"""
    function_name: str
    base_execution_time_ms: float  # Base time for 1MB input
    scaling_exponent: float  # η in t = α * input_size^η
    memory_requirement_mb: int
    cpu_intensity: float  # 0-1 scale
    r_squared: float = 0.0  # R² goodness of fit for the scaling law
    # Optional per-node overrides for base time (α) at 1MB
    per_node_base_time_ms: Dict[str, float] = None
    
    def predict_execution_time(self, input_size_mb: float, node_id: Optional[str] = None) -> float:
        """Predict execution time for given input size"""
        alpha = self.base_execution_time_ms
        if node_id and self.per_node_base_time_ms and node_id in self.per_node_base_time_ms:
            alpha = self.per_node_base_time_ms[node_id]
        return alpha * (input_size_mb ** self.scaling_exponent)

@dataclass
class NodeProfile:
    """Static profile for a node's performance characteristics"""
    node_id: str
    cpu_cores: int
    memory_gb: float
    cpu_efficiency: float  # Relative performance factor (0.5-2.0)
    memory_efficiency: float  # Memory access efficiency
    network_bandwidth_mbps: float
    
    def get_effective_cpu_cores(self) -> float:
        """Get effective CPU cores accounting for efficiency"""
        return self.cpu_cores * self.cpu_efficiency

@dataclass
class NetworkProfile:
    """Static profile for network transfer characteristics"""
    src_node_id: str
    dst_node_id: str
    base_latency_ms: float  # Base network latency
    bandwidth_mbps: float  # Effective bandwidth between nodes
    congestion_factor: float  # Typical congestion overhead (1.0-2.0)
    
    def predict_transfer_time(self, data_size_mb: float) -> float:
        """Predict transfer time for given data size"""
        # Transfer time = latency + (data_size / bandwidth)
        transfer_time_ms = (data_size_mb * 8) / (self.bandwidth_mbps / 1000)  # Convert to ms
        return self.base_latency_ms + (transfer_time_ms * self.congestion_factor)

@dataclass
class StaticProfiles:
    """Container for all static profiling data"""
    function_profiles: Dict[str, FunctionProfile]
    node_profiles: Dict[str, NodeProfile]
    network_profiles: Dict[Tuple[str, str], NetworkProfile]
    calibration_timestamp: str
    version: str = "1.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'version': self.version,
            'calibration_timestamp': self.calibration_timestamp,
            'function_profiles': {k: asdict(v) for k, v in self.function_profiles.items()},
            'node_profiles': {k: asdict(v) for k, v in self.node_profiles.items()},
            'network_profiles': {f"{k[0]}-{k[1]}": asdict(v) for k, v in self.network_profiles.items()}
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StaticProfiles':
        """Create from dictionary (JSON deserialization)"""
        function_profiles = {
            k: FunctionProfile(**v) for k, v in data.get('function_profiles', {}).items()
        }
        
        node_profiles = {
            k: NodeProfile(**v) for k, v in data.get('node_profiles', {}).items()
        }
        
        network_profiles = {}
        for key, profile_data in data.get('network_profiles', {}).items():
            src, dst = key.split('-', 1)
            network_profiles[(src, dst)] = NetworkProfile(**profile_data)
        
        return cls(
            function_profiles=function_profiles,
            node_profiles=node_profiles,
            network_profiles=network_profiles,
            calibration_timestamp=data.get('calibration_timestamp', 'unknown'),
            version=data.get('version', '1.0')
        )

class StaticProfilingManager:
    """Manages static profiling data - no runtime measurement"""
    
    def __init__(self, profiles_dir: str = "results/static_profiles"):
        self.profiles_dir = profiles_dir
        self.profiles: Optional[StaticProfiles] = None
        self.logger = logging.getLogger(__name__)
        
        # Ensure profiles directory exists
        os.makedirs(profiles_dir, exist_ok=True)
    
    def load_profiles(self, profile_name: str = "default") -> bool:
        """Load static profiles from file"""
        profile_file = os.path.join(self.profiles_dir, f"{profile_name}.json")
        
        if not os.path.exists(profile_file):
            self.logger.warning(f"Static profiles not found: {profile_file}")
            return False
        
        try:
            with open(profile_file, 'r') as f:
                data = json.load(f)
            
            self.profiles = StaticProfiles.from_dict(data)
            self.logger.info(f"Loaded static profiles: {len(self.profiles.function_profiles)} functions, "
                           f"{len(self.profiles.node_profiles)} nodes, "
                           f"{len(self.profiles.network_profiles)} network paths")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load static profiles: {e}")
            return False
    
    def save_profiles(self, profiles: StaticProfiles, profile_name: str = "default") -> bool:
        """Save static profiles to file"""
        profile_file = os.path.join(self.profiles_dir, f"{profile_name}.json")
        
        try:
            with open(profile_file, 'w') as f:
                json.dump(profiles.to_dict(), f, indent=2)
            
            self.logger.info(f"Saved static profiles to: {profile_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save static profiles: {e}")
            return False
    
    def predict_compute_cost(self, function_name: str, node_id: str, 
                           input_size_mb: float) -> Tuple[float, float]:
        """
        Predict compute cost using static profiles
        
        Returns:
            (predicted_time_ms, confidence)
            confidence = 1.0 if profiles exist, 0.0 otherwise
        """
        if not self.profiles:
            return 0.0, 0.0
        
        # Get function profile
        func_profile = self.profiles.function_profiles.get(function_name)
        if not func_profile:
            return 0.0, 0.0
        
        # Get node profile
        node_profile = self.profiles.node_profiles.get(node_id)
        if not node_profile:
            return 0.0, 0.0
        
        # Predict execution time
        base_time = func_profile.predict_execution_time(input_size_mb, node_id=node_id)
        
        # Adjust for node efficiency
        effective_time = base_time / node_profile.cpu_efficiency
        
        return effective_time, 1.0  # Full confidence for static profiles
    
    def predict_network_cost(self, src_node_id: str, dst_node_id: str, 
                           data_size_mb: float) -> Tuple[float, float]:
        """
        Predict network transfer cost using static profiles
        
        Returns:
            (predicted_time_ms, confidence)
        """
        if not self.profiles:
            return 0.0, 0.0
        
        # Get network profile
        network_profile = self.profiles.network_profiles.get((src_node_id, dst_node_id))
        if not network_profile:
            return 0.0, 0.0
        
        # Predict transfer time
        transfer_time = network_profile.predict_transfer_time(data_size_mb)
        
        return transfer_time, 1.0  # Full confidence for static profiles
    
    def get_function_profile(self, function_name: str) -> Optional[FunctionProfile]:
        """Get function profile if available"""
        if not self.profiles:
            return None
        return self.profiles.function_profiles.get(function_name)
    
    def get_node_profile(self, node_id: str) -> Optional[NodeProfile]:
        """Get node profile if available"""
        if not self.profiles:
            return None
        return self.profiles.node_profiles.get(node_id)
    
    def has_profiles(self) -> bool:
        """Check if profiles are loaded"""
        return self.profiles is not None

class CalibrationRunner:
    """Runs calibration phase to measure and create static profiles"""
    
    def __init__(self, function_container_manager, profiles_dir: str = "results/static_profiles"):
        self.function_container_manager = function_container_manager
        self.profiles_dir = profiles_dir
        self.logger = logging.getLogger(__name__)
        
        # Initialize static profiling manager
        self.static_profiler = StaticProfilingManager(profiles_dir)
        
        # Ensure profiles directory exists
        os.makedirs(profiles_dir, exist_ok=True)
        
        # DAG execution support
        self.dag_executor = None
        self.dag = None
    
    def run_calibration(self, functions: List[str], input_sizes_mb: List[float] = [0.1, 0.5, 1.0, 2.0, 5.0],
                       samples_per_measurement: int = 5, dag_file: str = None) -> StaticProfiles:
        """
        Run calibration phase to measure function and node performance
        
        Args:
            functions: List of function names to calibrate
            input_sizes_mb: List of input sizes to test
            samples_per_measurement: Number of samples per measurement for averaging
            dag_file: Optional DAG file path for DAG-based calibration
        """
        self.logger.info("Starting calibration phase...")
        
        # Get available nodes
        nodes = self.function_container_manager.get_nodes()
        if not nodes:
            raise Exception("No nodes available for calibration")
        
        # Load DAG if provided
        if dag_file:
            self._load_dag(dag_file)
            if self.dag:
                self.logger.info(f"Loaded DAG from {dag_file}")
                # Extract functions from DAG if not provided
                if not functions:
                    functions = self._extract_functions_from_dag()
        
        # Measure function profiles
        function_profiles = self._calibrate_functions(functions, input_sizes_mb, samples_per_measurement)
        
        # Measure node profiles
        node_profiles = self._calibrate_nodes(nodes)
        
        # Measure network profiles
        network_profiles = self._calibrate_network(nodes)
        
        # Enrich function profiles with per-node α by re-measuring smallest size on each node
        try:
            for function_name, profile in function_profiles.items():
                per_node: Dict[str, float] = {}
                containers = self.function_container_manager.get_running_containers(function_name) or []
                nodes_with_func = sorted(list({getattr(c, 'node_id', 'unknown') for c in containers}))
                if not nodes_with_func:
                    continue
                small_size = min(input_sizes_mb) if input_sizes_mb else 1.0
                print(f"  🔧 Measuring per-node α for {function_name} using {small_size}MB input...")
                for node_id in nodes_with_func:
                    times = self._measure_function_execution(function_name, node_id, small_size, max(1, int(samples_per_measurement)))
                    if times:
                        avg_time = statistics.mean(times)
                        eta = profile.scaling_exponent if profile.scaling_exponent else 1.0
                        alpha_node = avg_time / (small_size ** eta)
                        per_node[node_id] = alpha_node
                        print(f"    📈 Per-node α for {function_name} on {node_id}: {alpha_node:.3f}ms")
                if per_node:
                    profile.per_node_base_time_ms = per_node
                    # Also print per-node coefficient lines (α, η) for clarity
                    for node_id, alpha_node in per_node.items():
                        print(f"  ▶ Coefficients on {node_id}: α={alpha_node:.3f}ms, η={profile.scaling_exponent:.3f}")
        except Exception as e:
            self.logger.warning(f"Per-node profiling step failed: {e}")

        # Create static profiles
        profiles = StaticProfiles(
            function_profiles=function_profiles,
            node_profiles=node_profiles,
            network_profiles=network_profiles,
            calibration_timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        self.logger.info("Calibration completed successfully")
        return profiles
    
    def create_default_profiles(self, functions: List[str]) -> StaticProfiles:
        """Create default profiles when calibration fails or containers unavailable"""
        self.logger.info("Creating default profiles (no calibration performed)")
        
        # Create default function profiles
        function_profiles = {}
        for function_name in functions:
            function_profiles[function_name] = self._create_default_function_profile(function_name)
        
        # Create default node profiles
        nodes = self.function_container_manager.get_nodes()
        node_profiles = {}
        for node in nodes:
            node_id = node.node_id if hasattr(node, 'node_id') else str(node)
            node_profiles[node_id] = NodeProfile(
                node_id=node_id,
                cpu_cores=node.cpu_cores if hasattr(node, 'cpu_cores') else 4,
                memory_gb=node.memory_gb if hasattr(node, 'memory_gb') else 8.0,
                cpu_efficiency=1.0,  # Default efficiency
                memory_efficiency=1.0,  # Default efficiency
                network_bandwidth_mbps=1000.0  # Default 1Gbps
            )
        
        # Create default network profiles
        network_profiles = {}
        for i, src_node in enumerate(nodes):
            for j, dst_node in enumerate(nodes):
                if i != j:
                    src_id = src_node.node_id if hasattr(src_node, 'node_id') else str(src_node)
                    dst_id = dst_node.node_id if hasattr(dst_node, 'node_id') else str(dst_node)
                    
                    network_profiles[(src_id, dst_id)] = NetworkProfile(
                        src_node_id=src_id,
                        dst_node_id=dst_id,
                        base_latency_ms=1.0,  # Default 1ms latency
                        bandwidth_mbps=1000.0,  # Default 1Gbps
                        congestion_factor=1.2  # 20% overhead
                    )
        
        return StaticProfiles(
            function_profiles=function_profiles,
            node_profiles=node_profiles,
            network_profiles=network_profiles,
            calibration_timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
        )
    
    def _calibrate_functions(self, functions: List[str], input_sizes_mb: List[float], 
                           samples_per_measurement: int) -> Dict[str, FunctionProfile]:
        """Calibrate function execution characteristics using batch fitting"""
        function_profiles = {}
        
        for function_name in functions:
            print(f"📊 Calibrating function: {function_name}")
            
            # Find a suitable node for calibration
            calibration_node = self._find_calibration_node(function_name)
            if not calibration_node:
                print(f"⚠️  No suitable node found for {function_name}, using defaults")
                function_profiles[function_name] = self._create_default_function_profile(function_name)
                continue
            
            # Collect ALL measurements first (batch approach)
            all_measurements = []
            containers = self.function_container_manager.get_running_containers(function_name) or []
            nodes_with_func = sorted(list({getattr(c, 'node_id', 'unknown') for c in containers}))
            if not nodes_with_func:
                nodes_with_func = [calibration_node]
            
            print(f"  📋 Collecting measurements across {len(nodes_with_func)} nodes...")
            for node_for_test in nodes_with_func:
                for i, input_size_mb in enumerate(input_sizes_mb, 1):
                    print(f"    🔍 Testing {input_size_mb}MB input ({i}/{len(input_sizes_mb)}) on node {node_for_test}...")
                    times = self._measure_function_execution(
                        function_name, node_for_test, input_size_mb, samples_per_measurement
                    )
                    if times:
                        avg_time = statistics.mean(times)
                        all_measurements.append((input_size_mb, avg_time))
                        print(f"      ✅ {input_size_mb}MB: {avg_time:.3f}ms (avg of {len(times)} samples) on node {node_for_test}")
                    else:
                        print(f"      ❌ {input_size_mb}MB: No successful measurements on node {node_for_test}")
            
            # Batch fitting: fit once using all collected measurements
            if len(all_measurements) >= 2:
                print(f"  🧮 Fitting scaling law using {len(all_measurements)} measurements...")
                alpha, eta, r_squared = self._fit_scaling_law_with_quality(all_measurements)
                
                # Estimate memory and CPU requirements
                memory_mb, cpu_intensity = self._estimate_resource_requirements(function_name, all_measurements)
                
                function_profiles[function_name] = FunctionProfile(
                    function_name=function_name,
                    base_execution_time_ms=alpha,
                    scaling_exponent=eta,
                    memory_requirement_mb=memory_mb,
                    cpu_intensity=cpu_intensity,
                    r_squared=r_squared
                )
                
                print(f"  ✅ Batch-fitted profile: α={alpha:.3f}ms, η={eta:.3f}, R²={r_squared:.3f}")
                print(f"     Memory={memory_mb}MB, CPU={cpu_intensity:.2f}")
                print(f"     Fitting quality: {'Excellent' if r_squared > 0.95 else 'Good' if r_squared > 0.85 else 'Fair' if r_squared > 0.7 else 'Poor'}")
            else:
                print(f"⚠️  Insufficient measurements for {function_name}, using defaults")
                function_profiles[function_name] = self._create_default_function_profile(function_name)
        
        return function_profiles
    
    def _calibrate_nodes(self, nodes: List) -> Dict[str, NodeProfile]:
        """Calibrate node performance characteristics"""
        node_profiles = {}
        
        for node in nodes:
            node_id = node.node_id if hasattr(node, 'node_id') else str(node)
            
            # Measure node performance using a standard benchmark
            cpu_efficiency = self._measure_cpu_efficiency(node)
            memory_efficiency = self._measure_memory_efficiency(node)
            
            node_profiles[node_id] = NodeProfile(
                node_id=node_id,
                cpu_cores=node.cpu_cores if hasattr(node, 'cpu_cores') else 4,
                memory_gb=node.memory_gb if hasattr(node, 'memory_gb') else 8.0,
                cpu_efficiency=cpu_efficiency,
                memory_efficiency=memory_efficiency,
                network_bandwidth_mbps=1000.0  # Default, could be measured
            )
            
            self.logger.info(f"Node {node_id}: CPU efficiency={cpu_efficiency:.3f}, "
                           f"Memory efficiency={memory_efficiency:.3f}")
        
        return node_profiles
    
    def _calibrate_network(self, nodes: List) -> Dict[Tuple[str, str], NetworkProfile]:
        """Calibrate network transfer characteristics"""
        network_profiles = {}
        
        # For now, create default network profiles
        # In a real implementation, you would measure actual network performance
        for i, src_node in enumerate(nodes):
            for j, dst_node in enumerate(nodes):
                if i != j:
                    src_id = src_node.node_id if hasattr(src_node, 'node_id') else str(src_node)
                    dst_id = dst_node.node_id if hasattr(dst_node, 'node_id') else str(dst_node)
                    
                    network_profiles[(src_id, dst_id)] = NetworkProfile(
                        src_node_id=src_id,
                        dst_node_id=dst_id,
                        base_latency_ms=1.0,  # Default 1ms latency
                        bandwidth_mbps=1000.0,  # Default 1Gbps
                        congestion_factor=1.2  # 20% overhead
                    )
        
        return network_profiles
    
    def _find_calibration_node(self, function_name: str):
        """Find a suitable node for calibrating a function"""
        # Get available containers for this function
        containers = self.function_container_manager.get_running_containers(function_name)
        if containers:
            return containers[0].node_id
        
        # Fallback to any available node
        nodes = self.function_container_manager.get_nodes()
        return nodes[0].node_id if nodes else None
    
    def _find_calibration_container(self, function_name: str, node_id: Optional[str] = None):
        """Find a suitable container for calibrating a function, optionally pinned to a node"""
        containers = self.function_container_manager.get_running_containers(function_name)
        if containers:
            if node_id is not None:
                for c in containers:
                    if getattr(c, 'node_id', None) == node_id:
                        return c
            return containers[0]
        # Fallback: search any container list for the target node
        for _, container_list in getattr(self.function_container_manager, 'function_containers', {}).items():
            for c in container_list:
                if node_id is None or getattr(c, 'node_id', None) == node_id:
                    return c
        return None
    
    def _measure_function_execution(self, function_name: str, node_id: str, 
                                  input_size_mb: float, samples: int) -> List[float]:
        """Measure raw function execution time without any scheduler overhead"""
        # Generate appropriate test data based on function type
        test_data = self._generate_test_data(function_name, input_size_mb)
        
        execution_times = []
        for i in range(samples):
            try:
                # Find a container for this function on the requested node
                container = self._find_calibration_container(function_name, node_id=node_id)
                if not container:
                    self.logger.warning(f"No containers available for {function_name}")
                    continue
                
                node_id = container.node_id if hasattr(container, 'node_id') else 'unknown'
                print(f"    📝 Sample {i+1}/{samples} on node {node_id}...")
                
                # Measure raw execution time (no scheduler overhead)
                start_time = time.time()
                
                # Send direct request to container
                response = self._send_raw_request_to_container(container, function_name, test_data, input_size_mb)
                
                end_time = time.time()
                execution_time_ms = (end_time - start_time) * 1000.0
                
                if response and response.get('status') != 'error':
                    # Use the raw execution time (no scheduler components involved)
                    execution_times.append(execution_time_ms)
                    print(f"      ✅ {execution_time_ms:.3f}ms on node {node_id}")
                else:
                    error_msg = response.get('message', 'Unknown error') if response else 'No response'
                    print(f"      ❌ Failed on node {node_id}: {error_msg}")
                
                # Small delay between measurements
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.warning(f"      ❌ Exception: {e}")
                continue
        
        return execution_times
    
    def _generate_test_data(self, function_name: str, input_size_mb: float) -> bytes:
        """Generate appropriate test data based on function type"""
        if function_name in ['recognizer__upload', 'recognizer__adult', 'recognizer__violence', 'recognizer__extract', 'recognizer__mosaic']:
            # For image processing functions, create a simple test image
            return self._create_test_image(input_size_mb)
        elif function_name in ['recognizer__translate', 'recognizer__censor']:
            # For text processing functions, create text data
            return self._create_test_text(input_size_mb)
        else:
            # Default: create binary data
            return b"X" * int(input_size_mb * 1024 * 1024)
    
    def _create_test_image(self, size_mb: float) -> bytes:
        """Create a simple test image of specified size"""
        try:
            from PIL import Image
            import io
            
            # Calculate dimensions to achieve target size
            # Simple formula: width * height * 3 (RGB) ≈ size_mb
            target_bytes = int(size_mb * 1024 * 1024)
            pixels = target_bytes // 3  # RGB = 3 bytes per pixel
            
            # Create square image
            side_length = int(pixels ** 0.5)
            if side_length < 1:
                side_length = 1
            
            # Create a simple test image
            img = Image.new('RGB', (side_length, side_length), color='red')
            
            # Convert to bytes
            img_bytes = io.BytesIO()
            img.save(img_bytes, format='PNG')
            img_bytes.seek(0)
            
            return img_bytes.getvalue()
            
        except ImportError:
            # Fallback: create a minimal PNG header + data
            self.logger.warning("PIL not available, using fallback image data")
            return self._create_fallback_image(size_mb)
        except Exception as e:
            self.logger.warning(f"Failed to create test image: {e}, using fallback")
            return self._create_fallback_image(size_mb)
    
    def _create_fallback_image(self, size_mb: float) -> bytes:
        """Create fallback image data when PIL is not available"""
        # Minimal PNG header + data to approximate size
        png_header = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\tpHYs\x00\x00\x0b\x13\x00\x00\x0b\x13\x01\x00\x9a\x9c\x18\x00\x00\x00\nIDATx\x9cc```\x00\x00\x00\x04\x00\x01\xdd\x8d\xb4\x1c\x00\x00\x00\x00IEND\xaeB`\x82'
        
        target_bytes = int(size_mb * 1024 * 1024)
        if len(png_header) >= target_bytes:
            return png_header[:target_bytes]
        
        # Add padding to reach target size
        padding = b'\x00' * (target_bytes - len(png_header))
        return png_header + padding
    
    def _create_test_text(self, size_mb: float) -> bytes:
        """Create test text data of specified size"""
        target_bytes = int(size_mb * 1024 * 1024)
        text_content = "This is test text for calibration. " * (target_bytes // 35)
        return text_content[:target_bytes].encode('utf-8')
    
    def _send_raw_request_to_container(self, container, function_name: str, test_data: bytes, input_size_mb: float):
        """Send raw request to container using shared utilities (no scheduler overhead)"""
        try:
            # Use the shared orchestrator utilities for direct container communication
            from scheduler.shared.orchestrator_utils import OrchestratorUtils
            from scheduler.shared.dag_executor import DAGExecutor
            
            # Create a mock DAG executor to use its input preparation logic
            mock_orchestrator = MockOrchestrator(self.function_container_manager)
            dag_executor = DAGExecutor(mock_orchestrator)
            
            # Create a mock graph and context for input preparation
            mock_graph = {
                'dependencies': {function_name: []},  # No dependencies for individual calibration
                'nodes': {function_name: {'id': function_name, 'type': 'task'}}
            }
            
            # For text functions, provide text input instead of image input
            if function_name in ["recognizer__translate", "recognizer__censor"]:
                text_content = test_data.decode('utf-8', errors='ignore')
                if not text_content.strip():
                    text_content = "This is test text for calibration purposes."
                mock_context = {'inputs': {'text': text_content}}
            else:
                mock_context = {'inputs': {'img': test_data}}
            
            mock_node_results = {}
            
            # For text functions, bypass the shared logic and create the correct input directly
            if function_name in ["recognizer__translate", "recognizer__censor"]:
                text_content = test_data.decode('utf-8', errors='ignore')
                if not text_content.strip():
                    text_content = "This is test text for calibration purposes."
                prepared_input = {"text": text_content}
            else:
                # Use the shared input preparation logic for image functions
                try:
                    prepared_input = dag_executor.prepare_input_for_node(
                        function_name, mock_graph, mock_context, mock_node_results
                    )
                except KeyError as e:
                    # If the shared logic fails (e.g., looking for 'img' key), use raw bytes
                    prepared_input = test_data
            
            # Determine timeout based on function type and input size
            if function_name in ["recognizer__translate", "recognizer__censor"]:
                # Much longer timeout for text functions - they can be slow
                timeout = max(120, min(600, int(input_size_mb * 120)))  # 2-10 minutes
                print(f"  ⏱️  Using {timeout}s timeout for {input_size_mb}MB text input")
                print(f"  📝 Sending {len(str(prepared_input))} characters to {function_name}")
            else:
                timeout = max(60, min(300, int(input_size_mb * 60)))
                print(f"  ⏱️  Using {timeout}s timeout for {input_size_mb}MB image input")
            
            # Send request directly to container without any scheduling logic
            response = OrchestratorUtils.send_request_to_container(
                container=container,
                function_name=function_name,
                body=prepared_input,
                timeline=None,  # No timeline tracking for raw measurement
                timeout=timeout
            )
            
            return response
            
        except Exception as e:
            self.logger.warning(f"Failed to send raw request to container: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _send_calibration_request(self, container, function_name: str, test_data: bytes):
        """Send calibration request to container"""
        try:
            import requests
            import time
            
            # Get container URL - use the container's actual endpoint
            if hasattr(container, 'endpoint') and container.endpoint:
                container_url = container.endpoint
            elif hasattr(container, 'port') and container.port:
                container_url = f"http://{container.node_id}:{container.port}/invoke"
            else:
                # Fallback to default port
                container_url = f"http://{container.node_id}:8080/invoke"
            
            # Prepare request
            payload = {
                'function_name': function_name,
                'body': test_data.decode('latin-1') if isinstance(test_data, bytes) else test_data
            }
            
            # Send request with timeout
            response = requests.post(container_url, json=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.warning(f"Container request failed: {response.status_code}")
                return {'status': 'error', 'message': f'HTTP {response.status_code}'}
                
        except Exception as e:
            self.logger.warning(f"Failed to send calibration request: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _measure_cpu_efficiency(self, node) -> float:
        """Measure CPU efficiency of a node"""
        # Simplified measurement - in practice you would run CPU benchmarks
        # For now, return a default efficiency based on node characteristics
        cpu_cores = node.cpu_cores if hasattr(node, 'cpu_cores') else 4
        return min(2.0, max(0.5, 1.0 + (cpu_cores - 4) * 0.1))  # Scale with CPU cores
    
    def _measure_memory_efficiency(self, node) -> float:
        """Measure memory efficiency of a node"""
        # Simplified measurement - in practice you would run memory benchmarks
        # For now, return a default efficiency
        return 1.0  # Default efficiency
    
    def _fit_scaling_law(self, measurements: List[Tuple[float, float]]) -> Tuple[float, float]:
        """Fit scaling law t = α * input_size^η to measurements"""
        alpha, eta, _ = self._fit_scaling_law_with_quality(measurements)
        return alpha, eta
    
    def _fit_scaling_law_with_quality(self, measurements: List[Tuple[float, float]]) -> Tuple[float, float, float]:
        """Fit scaling law t = α * input_size^η to measurements and return R²"""
        if len(measurements) < 2:
            return 100.0, 1.0, 0.0  # Default values with poor fit
        
        # Simple linear regression on log-transformed data
        # log(t) = log(α) + η * log(input_size)
        log_inputs = [math.log(input_size) for input_size, _ in measurements]
        log_times = [math.log(time_ms) for _, time_ms in measurements]
        
        # Calculate regression coefficients
        n = len(measurements)
        sum_x = sum(log_inputs)
        sum_y = sum(log_times)
        sum_xy = sum(x * y for x, y in zip(log_inputs, log_times))
        sum_x2 = sum(x * x for x in log_inputs)
        
        # Calculate slope (η) and intercept (log(α))
        eta = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        log_alpha = (sum_y - eta * sum_x) / n
        
        alpha = math.exp(log_alpha)
        
        # Calculate R² (coefficient of determination)
        # R² = 1 - (SS_res / SS_tot)
        # SS_res = sum of squared residuals
        # SS_tot = total sum of squares
        
        # Calculate predicted values
        predicted_log_times = [log_alpha + eta * x for x in log_inputs]
        
        # Calculate mean of observed values
        mean_log_time = sum_y / n
        
        # Calculate sum of squared residuals
        ss_res = sum((y_obs - y_pred) ** 2 for y_obs, y_pred in zip(log_times, predicted_log_times))
        
        # Calculate total sum of squares
        ss_tot = sum((y_obs - mean_log_time) ** 2 for y_obs in log_times)
        
        # Calculate R²
        if ss_tot == 0:
            r_squared = 1.0  # Perfect fit if no variation
        else:
            r_squared = 1.0 - (ss_res / ss_tot)
        
        return alpha, eta, r_squared
    
    def _estimate_resource_requirements(self, function_name: str, 
                                      measurements: List[Tuple[float, float]]) -> Tuple[int, float]:
        """Estimate memory and CPU requirements based on function characteristics and execution patterns"""
        
        # Base estimates by function type
        base_memory_mb = 256  # Start with 256MB base
        base_cpu_intensity = 0.3  # Start with 30% CPU intensity
        
        # Adjust based on function name patterns
        if 'recognizer' in function_name.lower():
            if 'adult' in function_name.lower() or 'violence' in function_name.lower():
                # ML model inference - higher memory and CPU
                base_memory_mb = 512
                base_cpu_intensity = 0.8
            elif 'extract' in function_name.lower():
                # OCR/text extraction - moderate resources
                base_memory_mb = 384
                base_cpu_intensity = 0.6
            elif 'mosaic' in function_name.lower():
                # Image processing - moderate resources
                base_memory_mb = 384
                base_cpu_intensity = 0.5
            else:
                # General recognizer functions
                base_memory_mb = 256
                base_cpu_intensity = 0.4
        elif 'translate' in function_name.lower():
            # Text translation - moderate resources
            base_memory_mb = 256
            base_cpu_intensity = 0.4
        elif 'censor' in function_name.lower():
            # Text filtering - low resources
            base_memory_mb = 128
            base_cpu_intensity = 0.2
        elif 'upload' in function_name.lower():
            # File upload - minimal processing
            base_memory_mb = 128
            base_cpu_intensity = 0.1
        
        # Adjust based on execution time characteristics
        if measurements:
            # Calculate average execution time for 1MB input
            avg_time_1mb = 0
            count = 0
            for input_size, exec_time in measurements:
                if input_size > 0:
                    # Normalize to 1MB equivalent
                    normalized_time = exec_time / (input_size ** 0.5)  # Rough normalization
                    avg_time_1mb += normalized_time
                    count += 1
            
            if count > 0:
                avg_time_1mb /= count
                
                # Adjust memory based on execution time (longer = more complex = more memory)
                if avg_time_1mb > 500:  # > 500ms for 1MB
                    base_memory_mb = int(base_memory_mb * 2.0)  # Double memory
                    base_cpu_intensity = min(1.0, base_cpu_intensity * 1.5)  # Increase CPU
                elif avg_time_1mb > 200:  # > 200ms for 1MB
                    base_memory_mb = int(base_memory_mb * 1.5)  # 1.5x memory
                    base_cpu_intensity = min(1.0, base_cpu_intensity * 1.2)  # Increase CPU
                elif avg_time_1mb < 50:  # < 50ms for 1MB
                    base_memory_mb = int(base_memory_mb * 0.8)  # Reduce memory
                    base_cpu_intensity = max(0.1, base_cpu_intensity * 0.8)  # Reduce CPU
        
        # Ensure reasonable bounds
        memory_mb = max(64, min(2048, base_memory_mb))  # 64MB to 2GB
        cpu_intensity = max(0.1, min(1.0, base_cpu_intensity))  # 10% to 100%
        
        return memory_mb, cpu_intensity
    
    def _create_default_function_profile(self, function_name: str) -> FunctionProfile:
        """Create default function profile when calibration fails"""
        memory_mb, cpu_intensity = self._estimate_resource_requirements(function_name, [])
        
        return FunctionProfile(
            function_name=function_name,
            base_execution_time_ms=100.0,  # Default 100ms for 1MB
            scaling_exponent=1.0,  # Linear scaling
            memory_requirement_mb=memory_mb,
            cpu_intensity=cpu_intensity,
            r_squared=0.0  # No fit quality for default profiles
        )
    
    def _load_dag(self, dag_file: str) -> None:
        """Load DAG from file and initialize DAG executor"""
        try:
            import yaml
            from functions.dag_loader import parse_workflow
            from scheduler.shared.dag_executor import DAGExecutor
            
            # Load DAG file
            with open(dag_file, 'r') as f:
                self.dag = yaml.safe_load(f)
            
            # Parse workflow to get functions and dependencies
            dependencies, dag_data, all_functions, edges = parse_workflow(dag_file)
            
            # Create a mock orchestrator for DAG execution
            mock_orchestrator = MockOrchestrator(self.function_container_manager)
            self.dag_executor = DAGExecutor(mock_orchestrator)
            
            self.logger.info(f"Successfully loaded DAG with {len(all_functions)} functions")
            
        except Exception as e:
            self.logger.warning(f"Failed to load DAG from {dag_file}: {e}")
            self.dag = None
            self.dag_executor = None
    
    def _extract_functions_from_dag(self) -> List[str]:
        """Extract function names from loaded DAG"""
        if not self.dag or 'workflow' not in self.dag:
            return []
        
        functions = []
        for node in self.dag['workflow'].get('nodes', []):
            if node.get('type') == 'task':
                functions.append(node['id'])
        
        return functions
    
    def run_dag_calibration(self, input_sizes_mb: List[float] = [0.1, 0.5, 1.0, 2.0, 5.0],
                           samples_per_measurement: int = 5) -> StaticProfiles:
        """
        Run calibration by executing the entire DAG workflow
        
        Args:
            input_sizes_mb: List of input sizes to test
            samples_per_measurement: Number of samples per measurement for averaging
        """
        if not self.dag or not self.dag_executor:
            raise Exception("DAG not loaded. Call _load_dag() first.")
        
        self.logger.info("Starting DAG-based calibration...")
        
        # Get available nodes
        nodes = self.function_container_manager.get_nodes()
        if not nodes:
            raise Exception("No nodes available for calibration")
        
        # Extract functions from DAG
        functions = self._extract_functions_from_dag()
        
        # Measure function profiles by executing DAG
        function_profiles = self._calibrate_functions_via_dag(functions, input_sizes_mb, samples_per_measurement)
        
        # Measure node profiles
        node_profiles = self._calibrate_nodes(nodes)
        
        # Measure network profiles
        network_profiles = self._calibrate_network(nodes)
        
        # Create static profiles
        profiles = StaticProfiles(
            function_profiles=function_profiles,
            node_profiles=node_profiles,
            network_profiles=network_profiles,
            calibration_timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        self.logger.info("DAG-based calibration completed successfully")
        return profiles
    
    def _calibrate_functions_via_dag(self, functions: List[str], input_sizes_mb: List[float], 
                                   samples_per_measurement: int) -> Dict[str, FunctionProfile]:
        """Calibrate function execution characteristics by running the full DAG using batch fitting"""
        function_profiles = {}
        
        for function_name in functions:
            print(f"📊 Calibrating function via DAG: {function_name}")
            
            # Collect ALL measurements first (batch approach)
            all_measurements = []
            print(f"  📋 Collecting measurements for DAG execution...")
            for i, input_size_mb in enumerate(input_sizes_mb, 1):
                print(f"    🔍 Testing {input_size_mb}MB input ({i}/{len(input_sizes_mb)})...")
                times = self._measure_dag_execution(
                    function_name, input_size_mb, samples_per_measurement
                )
                if times:
                    avg_time = statistics.mean(times)
                    all_measurements.append((input_size_mb, avg_time))
                    print(f"      ✅ {input_size_mb}MB: {avg_time:.3f}ms (avg of {len(times)} samples)")
                else:
                    print(f"      ❌ {input_size_mb}MB: No successful measurements")
            
            # Batch fitting: fit once using all collected measurements
            if len(all_measurements) >= 2:
                print(f"  🧮 Fitting scaling law using {len(all_measurements)} measurements...")
                alpha, eta, r_squared = self._fit_scaling_law_with_quality(all_measurements)
                
                # Estimate memory and CPU requirements
                memory_mb, cpu_intensity = self._estimate_resource_requirements(function_name, all_measurements)
                
                function_profiles[function_name] = FunctionProfile(
                    function_name=function_name,
                    base_execution_time_ms=alpha,
                    scaling_exponent=eta,
                    memory_requirement_mb=memory_mb,
                    cpu_intensity=cpu_intensity,
                    r_squared=r_squared
                )
                
                print(f"  ✅ Batch-fitted DAG profile: α={alpha:.3f}ms, η={eta:.3f}, R²={r_squared:.3f}")
                print(f"     Memory={memory_mb}MB, CPU={cpu_intensity:.2f}")
                print(f"     Fitting quality: {'Excellent' if r_squared > 0.95 else 'Good' if r_squared > 0.85 else 'Fair' if r_squared > 0.7 else 'Poor'}")
            else:
                print(f"⚠️  Insufficient measurements for {function_name}, using defaults")
                function_profiles[function_name] = self._create_default_function_profile(function_name)
        
        return function_profiles
    
    def _measure_dag_execution(self, target_function: str, input_size_mb: float, samples: int) -> List[float]:
        """Measure function execution time by running the full DAG and extracting timing for target function"""
        execution_times = []
        
        for i in range(samples):
            try:
                # Generate test data
                test_data = self._generate_test_data(target_function, input_size_mb)
                
                # Prepare DAG context
                context = {
                    'inputs': {'img': test_data}
                }
                
                # Execute DAG and measure timing
                start_time = time.time()
                
                # Use the DAG executor to run the workflow
                request_id = f"calibration_{target_function}_{i}_{int(time.time())}"
                result = self.dag_executor.execute_dag(self.dag, context, request_id)
                
                end_time = time.time()
                total_execution_time_ms = (end_time - start_time) * 1000.0
                
                # Extract timing for the target function from the timeline
                function_execution_time = self._extract_function_timing(request_id, target_function)
                
                if function_execution_time > 0:
                    execution_times.append(function_execution_time)
                else:
                    # Fallback to total execution time if function-specific timing not available
                    execution_times.append(total_execution_time_ms)
                
                # Small delay between measurements
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.warning(f"DAG execution measurement failed for {target_function}: {e}")
                continue
        
        return execution_times
    
    def _extract_function_timing(self, request_id: str, function_name: str) -> float:
        """Extract execution time for a specific function from the timeline"""
        try:
            # Get timeline from the mock orchestrator
            if hasattr(self.dag_executor.orchestrator, '_get_or_create_timeline'):
                timeline = self.dag_executor.orchestrator._get_or_create_timeline(request_id)
                
                if 'functions' in timeline:
                    for func_timeline in timeline['functions']:
                        if func_timeline.get('function') == function_name:
                            return func_timeline.get('exec_ms', 0.0)
            
            return 0.0
            
        except Exception as e:
            self.logger.debug(f"Failed to extract timing for {function_name}: {e}")
            return 0.0


class MockOrchestrator:
    """Mock orchestrator for DAG execution during calibration"""
    
    def __init__(self, function_container_manager):
        self.function_container_manager = function_container_manager
        self.timelines = {}
    
    def invoke_function(self, function_name: str, request_id: str, input_data: Any) -> Dict:
        """Mock function invocation that measures actual execution time"""
        try:
            # Find a container for this function
            container = self._find_calibration_container(function_name)
            if not container:
                return {'status': 'error', 'message': f'No container found for {function_name}'}
            
            # Measure actual execution time
            start_time = time.time()
            
            # Send request to container
            response = self._send_request_to_container(container, function_name, input_data)
            
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000.0
            
            # Update timeline
            self._update_timeline(request_id, function_name, execution_time_ms, response)
            
            return response
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _find_calibration_container(self, function_name: str, target_node_id: Optional[str] = None):
        """Find a suitable container for calibrating a function, optionally pinned to target_node_id"""
        containers = self.function_container_manager.get_running_containers(function_name) or []
        if target_node_id:
            for c in containers:
                if getattr(c, 'node_id', None) == target_node_id:
                    return c
        if containers:
            return containers[0]
        # Fallback search across any known containers
        for _, container_list in getattr(self.function_container_manager, 'function_containers', {}).items():
            for c in container_list:
                if not target_node_id or getattr(c, 'node_id', None) == target_node_id:
                    return c
        return None
    
    def _send_request_to_container(self, container, function_name: str, input_data: Any) -> Dict:
        """Send request to container using shared utilities"""
        try:
            from scheduler.shared.orchestrator_utils import OrchestratorUtils
            
            # Branch properly by function type
            if function_name in ["recognizer__translate", "recognizer__censor"]:
                # Text-based functions - send JSON with text field
                if isinstance(input_data, bytes):
                    text_content = input_data.decode('utf-8', errors='ignore')
                    if not text_content.strip():
                        text_content = "This is test text for calibration purposes."
                    body = {"text": text_content}
                elif isinstance(input_data, dict):
                    body = input_data
                else:
                    body = {"text": str(input_data)}
                
                # Scale timeout based on input size: 120 seconds per MB, minimum 2 minutes, maximum 10 minutes
                estimated_size_mb = len(str(body)) / (1024 * 1024) if isinstance(body, dict) else len(body) / (1024 * 1024)
                timeout = max(120, min(600, int(estimated_size_mb * 120)))
            else:
                # Image-based functions - send raw image bytes
                if isinstance(input_data, dict):
                    if 'img_b64' in input_data:
                        import base64
                        body = base64.b64decode(input_data['img_b64'])
                    else:
                        import json
                        body = json.dumps(input_data).encode('utf-8')
                elif isinstance(input_data, str):
                    body = input_data.encode('utf-8')
                else:
                    body = input_data
                
                # Scale timeout based on input size: 60 seconds per MB, minimum 60 seconds, maximum 5 minutes
                estimated_size_mb = len(body) / (1024 * 1024)
                timeout = max(60, min(300, int(estimated_size_mb * 60)))
            
            response = OrchestratorUtils.send_request_to_container(
                container=container,
                function_name=function_name,
                body=body,
                timeline=None,
                timeout=timeout
            )
            
            return response
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _update_timeline(self, request_id: str, function_name: str, execution_time_ms: float, response: Dict) -> None:
        """Update timeline with function execution results"""
        if request_id not in self.timelines:
            self.timelines[request_id] = {
                'functions': [],
                'timestamps': {}
            }
        
        timeline = self.timelines[request_id]
        
        # Add function entry
        function_entry = {
            'function': function_name,
            'request_id': f"{request_id}_{function_name}",
            'status': response.get('status', 'ok'),
            'message': response.get('message'),
            'node_id': response.get('node_id'),
            'container_id': response.get('container_id'),
            'exec_ms': execution_time_ms,
            'timestamps': {
                'invocation_start': time.time() - (execution_time_ms / 1000.0),
                'invocation_end': time.time()
            }
        }
        
        timeline['functions'].append(function_entry)
    
    def _get_or_create_timeline(self, request_id: str) -> Dict:
        """Get or create timeline for request"""
        if request_id not in self.timelines:
            self.timelines[request_id] = {
                'functions': [],
                'timestamps': {}
            }
        return self.timelines[request_id]
