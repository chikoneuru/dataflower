"""
Profiling Module for Ours Scheduler

Implements compute and network profiling to predict execution times and transfer costs.
Uses scaling models to guide placement decisions and bottleneck identification.

Models:
- Compute: t_comp(b,r) ≈ α_f,r · b^η_f,r / C_r
- Network: t_net(d,r,r') ≈ ℓ_r,r' + d / B_r→r'
"""

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


@dataclass
class ComputeProfile:
    """Compute execution profile for a function-node pair"""
    alpha: float  # Scaling coefficient
    eta: float    # Scaling exponent
    confidence: float  # Model confidence (0-1)
    sample_count: int  # Number of samples used
    last_updated: float
    r_squared: float = 0.0  # Model fit quality


@dataclass
class NetworkProfile:
    """Network transfer profile for a node-node pair"""
    base_latency: float  # ℓ_r,r' - base latency in ms
    bandwidth: float     # B_r→r' - effective bandwidth in MB/s
    confidence: float    # Model confidence (0-1)
    sample_count: int    # Number of samples used
    last_updated: float
    r_squared: float = 0.0  # Model fit quality


@dataclass
class ProfilingData:
    """Container for all profiling data"""
    compute_profiles: Dict[Tuple[str, str], ComputeProfile] = field(default_factory=dict)
    compute_samples: Dict[Tuple[str, str], List[Tuple[float, float, float]]] = field(default_factory=dict)
    network_profiles: Dict[Tuple[str, str], NetworkProfile] = field(default_factory=dict)
    network_samples: Dict[Tuple[str, str], List[Tuple[float, float]]] = field(default_factory=dict)


class ComputeProfiler:
    """Handles compute execution profiling"""
    
    def __init__(self, min_samples: int = 5):
        self.min_samples = min_samples
        self.logger = logging.getLogger(__name__)
        self.profiles = {}  # (func_name, node_id) -> ComputeProfile
        self.samples = {}   # (func_name, node_id) -> List[(input_size, exec_time, capacity)]
    
    def predict_execution_time(self, func_name: str, node_id: str, 
                              input_size: float, node_capacity: float) -> Tuple[float, float]:
        """
        Predict execution time using compute profile
        
        Args:
            func_name: Function name
            node_id: Target node ID
            input_size: Input size in MB
            node_capacity: Node capacity (CPU cores)
            
        Returns:
            (predicted_time_ms, confidence)
        """
        key = (func_name, node_id)
        
        if key not in self.profiles:
            # Fallback: estimate based on input size and capacity
            fallback_time = input_size * 100.0 / max(node_capacity, 1.0)  # Rough estimate
            return fallback_time, 0.0
        
        profile = self.profiles[key]
        if profile.sample_count < self.min_samples:
            # Not enough samples yet
            fallback_time = input_size * 100.0 / max(node_capacity, 1.0)
            return fallback_time, profile.confidence * 0.5  # Lower confidence
        
        # Use scaling model: α * input_size^η / capacity
        predicted_time = profile.alpha * (input_size ** profile.eta) / max(node_capacity, 1.0)
        return predicted_time, profile.confidence
    
    def add_sample(self, func_name: str, node_id: str, 
                   input_size: float, exec_time_ms: float, node_capacity: float):
        """Add a new execution sample"""
        key = (func_name, node_id)
        
        if key not in self.samples:
            self.samples[key] = []
        
        self.samples[key].append((input_size, exec_time_ms, node_capacity))
        
        # Fit model if we have enough samples
        if len(self.samples[key]) >= self.min_samples:
            self._fit_model(key)
    
    def _fit_model(self, key: Tuple[str, str]):
        """Fit scaling model to samples"""
        samples = self.samples[key]
        
        if len(samples) < self.min_samples:
            return
        
        try:
            # Extract data
            input_sizes = np.array([s[0] for s in samples])
            exec_times = np.array([s[1] for s in samples])
            capacities = np.array([s[2] for s in samples])
            
            # Normalize execution times by capacity
            normalized_times = exec_times * capacities
            
            # Fit power law model: log(t) = log(α) + η * log(b)
            # where t = exec_time * capacity, b = input_size
            log_inputs = np.log(input_sizes)
            log_times = np.log(normalized_times)
            
            # Linear regression in log space
            coeffs = np.polyfit(log_inputs, log_times, 1)
            eta = coeffs[0]
            log_alpha = coeffs[1]
            alpha = np.exp(log_alpha)
            
            # Calculate R-squared
            predicted_log_times = log_alpha + eta * log_inputs
            ss_res = np.sum((log_times - predicted_log_times) ** 2)
            ss_tot = np.sum((log_times - np.mean(log_times)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
            
            # Calculate confidence based on R-squared and sample count
            confidence = min(r_squared, 1.0) * min(len(samples) / (self.min_samples * 2), 1.0)
            
            # Create profile
            profile = ComputeProfile(
                alpha=alpha,
                eta=eta,
                confidence=confidence,
                sample_count=len(samples),
                last_updated=time.time(),
                r_squared=r_squared
            )
            
            self.profiles[key] = profile
            
            self.logger.info(f"Fitted compute profile for {key}: α={alpha:.3f}, η={eta:.3f}, "
                           f"R²={r_squared:.3f}, confidence={confidence:.3f}")
            
        except Exception as e:
            self.logger.error(f"Failed to fit compute model for {key}: {e}")


class NetworkProfiler:
    """Handles network transfer profiling"""
    
    def __init__(self, min_samples: int = 3):
        self.min_samples = min_samples
        self.logger = logging.getLogger(__name__)
        self.profiles = {}  # (src_node, dst_node) -> NetworkProfile
        self.samples = {}   # (src_node, dst_node) -> List[(data_size, transfer_time)]
    
    def predict_transfer_time(self, src_node: str, dst_node: str, 
                             data_size_mb: float) -> Tuple[float, float]:
        """
        Predict network transfer time
        
        Args:
            src_node: Source node ID
            dst_node: Destination node ID
            data_size_mb: Data size in MB
            
        Returns:
            (predicted_time_ms, confidence)
        """
        key = (src_node, dst_node)
        
        if key not in self.profiles:
            # Fallback: assume default cluster-wide averages
            fallback_time = data_size_mb * 10.0 + 5.0  # 10ms/MB + 5ms base
            return fallback_time, 0.0
        
        profile = self.profiles[key]
        if profile.sample_count < self.min_samples:
            # Not enough samples yet
            fallback_time = data_size_mb * 10.0 + 5.0
            return fallback_time, profile.confidence * 0.5
        
        # Use linear model: base_latency + data_size / bandwidth
        predicted_time = profile.base_latency + (data_size_mb / max(profile.bandwidth, 0.1))
        return predicted_time, profile.confidence
    
    def add_sample(self, src_node: str, dst_node: str, 
                   data_size_mb: float, transfer_time_ms: float):
        """Add a new transfer sample"""
        key = (src_node, dst_node)
        
        if key not in self.samples:
            self.samples[key] = []
        
        self.samples[key].append((data_size_mb, transfer_time_ms))
        
        # Fit model if we have enough samples
        if len(self.samples[key]) >= self.min_samples:
            self._fit_model(key)
    
    def _fit_model(self, key: Tuple[str, str]):
        """Fit linear model to samples"""
        samples = self.samples[key]
        
        if len(samples) < self.min_samples:
            return
        
        try:
            # Extract data
            data_sizes = np.array([s[0] for s in samples])
            transfer_times = np.array([s[1] for s in samples])
            
            # Fit linear model: t = base_latency + data_size / bandwidth
            # This is equivalent to: t = a + b * data_size
            # where a = base_latency, b = 1/bandwidth
            coeffs = np.polyfit(data_sizes, transfer_times, 1)
            base_latency = coeffs[1]  # y-intercept
            bandwidth = 1.0 / max(coeffs[0], 0.001)  # slope = 1/bandwidth
            
            # Calculate R-squared
            predicted_times = base_latency + data_sizes / bandwidth
            ss_res = np.sum((transfer_times - predicted_times) ** 2)
            ss_tot = np.sum((transfer_times - np.mean(transfer_times)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
            
            # Calculate confidence
            confidence = min(r_squared, 1.0) * min(len(samples) / (self.min_samples * 2), 1.0)
            
            # Create profile
            profile = NetworkProfile(
                base_latency=max(base_latency, 0.0),  # Ensure non-negative
                bandwidth=max(bandwidth, 0.1),        # Ensure positive
                confidence=confidence,
                sample_count=len(samples),
                last_updated=time.time(),
                r_squared=r_squared
            )
            
            self.profiles[key] = profile
            
            self.logger.info(f"Fitted network profile for {key}: latency={base_latency:.3f}ms, "
                           f"bandwidth={bandwidth:.3f}MB/s, R²={r_squared:.3f}, confidence={confidence:.3f}")
            
        except Exception as e:
            self.logger.error(f"Failed to fit network model for {key}: {e}")


class ProfilingManager:
    """Main profiling manager that coordinates compute and network profiling"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # Initialize profilers
        self.compute_profiler = ComputeProfiler(
            min_samples=self.config.get('min_compute_samples', 5)
        )
        self.network_profiler = NetworkProfiler(
            min_samples=self.config.get('min_network_samples', 3)
        )
        
        # Profile persistence
        self.profiles_dir = self.config.get('profiles_dir', 'results/profiles')
        self.profiles_file = os.path.join(self.profiles_dir, 'profiles.json')
        
        # Load existing profiles
        self.load_profiles()
    
    @property
    def profiles(self) -> ProfilingData:
        """Get combined profiling data"""
        return ProfilingData(
            compute_profiles=self.compute_profiler.profiles,
            compute_samples=self.compute_profiler.samples,
            network_profiles=self.network_profiler.profiles,
            network_samples=self.network_profiler.samples
        )
    
    def predict_compute_cost(self, func_name: str, node_id: str, 
                            input_size: float, node_capacity: float) -> Tuple[float, float]:
        """Predict compute execution cost"""
        return self.compute_profiler.predict_execution_time(
            func_name, node_id, input_size, node_capacity
        )
    
    def predict_network_cost(self, src_node: str, dst_node: str, 
                            data_size_mb: float) -> Tuple[float, float]:
        """Predict network transfer cost"""
        return self.network_profiler.predict_transfer_time(
            src_node, dst_node, data_size_mb
        )
    
    def update_compute_profile(self, func_name: str, node_id: str, 
                              input_size: float, exec_time_ms: float, node_capacity: float):
        """Update compute profile with new sample"""
        self.compute_profiler.add_sample(func_name, node_id, input_size, exec_time_ms, node_capacity)
    
    def update_network_profile(self, src_node: str, dst_node: str, 
                              data_size_mb: float, transfer_time_ms: float):
        """Update network profile with new sample"""
        self.network_profiler.add_sample(src_node, dst_node, data_size_mb, transfer_time_ms)
    
    def create_mock_profiles(self, function_names: List[str], node_ids: List[str]):
        """
        Create mock profiles for testing purposes.
        This allows testing the profiling integration without real execution data.
        """
        self.logger.info("Creating mock profiles for testing")
        
        # Create mock compute profiles
        for func_name in function_names:
            for node_id in node_ids:
                key = (func_name, node_id)
                
                # Mock scaling parameters: α=0.001, η=1.0 (linear scaling)
                mock_profile = ComputeProfile(
                    alpha=0.001,
                    eta=1.0,
                    confidence=0.8,  # High confidence for mock data
                    sample_count=5,
                    last_updated=time.time(),
                    r_squared=0.95
                )
                
                self.compute_profiler.profiles[key] = mock_profile
                
                # Create mock samples
                mock_samples = [
                    (1.0, 1.0, 2.0),   # 1MB -> 1ms, 2 CPU cores
                    (2.0, 2.0, 2.0),   # 2MB -> 2ms, 2 CPU cores
                    (5.0, 5.0, 2.0),   # 5MB -> 5ms, 2 CPU cores
                    (10.0, 10.0, 2.0), # 10MB -> 10ms, 2 CPU cores
                    (20.0, 20.0, 2.0)  # 20MB -> 20ms, 2 CPU cores
                ]
                self.compute_profiler.samples[key] = mock_samples
        
        # Create mock network profiles (disabled for now due to MinIO issues)
        # for src_node in node_ids:
        #     for dst_node in node_ids:
        #         if src_node != dst_node:
        #             key = (src_node, dst_node)
        #             mock_profile = NetworkProfile(
        #                 base_latency=5.0,  # 5ms base latency
        #                 bandwidth=100.0,   # 100 MB/s bandwidth
        #                 confidence=0.8,
        #                 sample_count=3,
        #                 last_updated=time.time(),
        #                 r_squared=0.95
        #             )
        #             self.network_profiler.profiles[key] = mock_profile
        
        self.logger.info(f"Created {len(self.compute_profiler.profiles)} mock compute profiles")
    
    def save_profiles(self):
        """Save profiles to disk"""
        try:
            os.makedirs(self.profiles_dir, exist_ok=True)
            
            # Convert to serializable format
            data = {
                'compute_profiles': {
                    f"{func}_{node}": {
                        'alpha': profile.alpha,
                        'eta': profile.eta,
                        'confidence': profile.confidence,
                        'sample_count': profile.sample_count,
                        'last_updated': profile.last_updated,
                        'r_squared': profile.r_squared
                    }
                    for (func, node), profile in self.compute_profiler.profiles.items()
                },
                'network_profiles': {
                    f"{src}_{dst}": {
                        'base_latency': profile.base_latency,
                        'bandwidth': profile.bandwidth,
                        'confidence': profile.confidence,
                        'sample_count': profile.sample_count,
                        'last_updated': profile.last_updated,
                        'r_squared': profile.r_squared
                    }
                    for (src, dst), profile in self.network_profiler.profiles.items()
                },
                'compute_samples': {
                    f"{func}_{node}": samples
                    for (func, node), samples in self.compute_profiler.samples.items()
                },
                'network_samples': {
                    f"{src}_{dst}": samples
                    for (src, dst), samples in self.network_profiler.samples.items()
                },
                'metadata': {
                    'saved_at': time.time(),
                    'version': '1.0'
                }
            }
            
            with open(self.profiles_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info(f"Profiles saved to {self.profiles_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save profiles: {e}")
    
    def load_profiles(self):
        """Load profiles from disk"""
        try:
            if not os.path.exists(self.profiles_file):
                self.logger.info("No existing profiles found, starting fresh")
                return
            
            with open(self.profiles_file, 'r') as f:
                data = json.load(f)
            
            # Load compute profiles
            for key_str, profile_data in data.get('compute_profiles', {}).items():
                func, node = key_str.split('_', 1)
                profile = ComputeProfile(
                    alpha=profile_data['alpha'],
                    eta=profile_data['eta'],
                    confidence=profile_data['confidence'],
                    sample_count=profile_data['sample_count'],
                    last_updated=profile_data['last_updated'],
                    r_squared=profile_data.get('r_squared', 0.0)
                )
                self.compute_profiler.profiles[(func, node)] = profile
            
            # Load network profiles
            for key_str, profile_data in data.get('network_profiles', {}).items():
                src, dst = key_str.split('_', 1)
                profile = NetworkProfile(
                    base_latency=profile_data['base_latency'],
                    bandwidth=profile_data['bandwidth'],
                    confidence=profile_data['confidence'],
                    sample_count=profile_data['sample_count'],
                    last_updated=profile_data['last_updated'],
                    r_squared=profile_data.get('r_squared', 0.0)
                )
                self.network_profiler.profiles[(src, dst)] = profile
            
            # Load samples
            for key_str, samples in data.get('compute_samples', {}).items():
                func, node = key_str.split('_', 1)
                self.compute_profiler.samples[(func, node)] = samples
            
            for key_str, samples in data.get('network_samples', {}).items():
                src, dst = key_str.split('_', 1)
                self.network_profiler.samples[(src, dst)] = samples
            
            self.logger.info(f"Loaded profiles from {self.profiles_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to load profiles: {e}")
