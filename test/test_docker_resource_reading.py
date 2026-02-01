#!/usr/bin/env python3
"""
Test Docker Resource Reading
Demonstrates reading actual Docker container resource constraints
"""

import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from provider import FunctionContainerManager
from scheduler.ours import create_integrated_cost_models


def test_docker_resource_reading():
    """Test reading Docker container resource constraints"""
    print("=== Docker Resource Reading Test ===")
    print()
    
    try:
        # Create container manager - this will discover nodes from Docker
        print("Creating function container manager...")
        container_manager = FunctionContainerManager()
        
        # Get discovered nodes
        nodes = container_manager.get_nodes()
        print(f"Discovered {len(nodes)} nodes from Docker:")
        print()
        
        for node in nodes:
            print(f"Node: {node.node_id}")
            print(f"  CPU Cores: {node.cpu_cores}")
            print(f"  Memory: {node.memory_gb:.1f} GB")
            print(f"  Available CPU: {node.available_cpu:.1f}")
            print(f"  Available Memory: {node.available_memory:.1f} GB")
            print(f"  Labels: {node.labels}")
            print(f"  Running Containers: {len(node.running_containers)}")
            print()
        
        # Test integration with cost models
        print("=== Cost Model Integration Test ===")
        integration = create_integrated_cost_models(container_manager)
        
        # Get cluster status
        status = integration.get_cluster_status()
        print("Cluster Status:")
        print(f"  Container Manager Nodes: {status['container_manager']['total_nodes']}")
        print(f"  Cost Model Profiles: {status['cost_models']['total_node_profiles']}")
        print(f"  Running Containers: {status['container_manager']['running_containers']}")
        print(f"  Average CPU Utilization: {status['cost_models']['average_cpu_utilization']:.1%}")
        print(f"  Average Memory Utilization: {status['cost_models']['average_memory_utilization']:.1%}")
        print()
        
        # Test task placement
        print("=== Task Placement Test ===")
        task = integration.create_task_profile(
            function_name="recognizer__adult",
            task_id="test-task-1",
            memory_requirement=0.5,  # 512MB
            cpu_requirement=0.5      # 0.5 CPU cores
        )
        
        print(f"Created task: {task.task_id}")
        print(f"  Memory requirement: {task.memory_requirement} GB")
        print(f"  CPU requirement: {task.cpu_requirement} cores")
        print()
        
        # Find suitable nodes
        suitable_nodes = integration.get_placement_candidates(task)
        print(f"Suitable nodes: {[n.node_id for n in suitable_nodes]}")
        
        # Get cost estimates for each suitable node
        for node in suitable_nodes:
            costs = integration.estimate_placement_cost(task, node.node_id)
            print(f"  {node.node_id}: Total cost = {costs['total']:.4f}s")
            print(f"    Compute: {costs['compute']:.4f}s")
            print(f"    Cold start: {costs['cold_start']:.4f}s")
            print(f"    Interference: {costs['interference']:.4f}s")
        print()
        
        # Find best placement
        best_node = integration.get_best_placement(task)
        if best_node:
            print(f"Best placement: {best_node}")
        else:
            print("No suitable placement found")
            
    except Exception as e:
        print(f"Test failed: {e}")
        print("This is expected if Docker containers are not running")
        print()
        print("To run this test:")
        print("1. Start containers with resource constraints:")
        print("   docker-compose -f examples/docker-compose-with-constraints.yml up -d")
        print("2. Run this test script")
        print("3. Check the resource readings")


def test_resource_extraction_directly():
    """Test resource extraction from running containers"""
    print("=== Direct Resource Extraction Test ===")
    print()
    
    try:
        import docker
        client = docker.from_env()
        
        # Find any running containers
        containers = client.containers.list()
        print(f"Found {len(containers)} running containers")
        print()
        
        # Create container manager to test resource extraction
        container_manager = FunctionContainerManager()
        
        for container in containers[:3]:  # Test first 3 containers
            print(f"Container: {container.name}")
            
            # Test resource extraction method
            cpu_cores, memory_gb = container_manager._extract_container_resources(container)
            
            print(f"  Extracted CPU cores: {cpu_cores}")
            print(f"  Extracted memory: {memory_gb:.1f} GB")
            
            # Show raw Docker info for comparison
            host_config = container.attrs.get('HostConfig', {})
            print(f"  Raw Docker config:")
            if 'Memory' in host_config:
                print(f"    Memory limit: {host_config['Memory'] / (1024**3):.1f} GB")
            if 'NanoCpus' in host_config:
                print(f"    CPU limit (NanoCpus): {host_config['NanoCpus'] / 1e9:.1f}")
            if 'CpuCount' in host_config:
                print(f"    CPU count: {host_config['CpuCount']}")
            print()
            
    except Exception as e:
        print(f"Direct test failed: {e}")


if __name__ == "__main__":
    print("Docker Resource Reading Test")
    print("=" * 50)
    print()
    
    test_docker_resource_reading()
    print("\n" + "=" * 50 + "\n")
    
    test_resource_extraction_directly()
    print("\n" + "=" * 50 + "\n")
    
    print("Test completed!")
    print()
    print("Note: For full testing, run containers with resource constraints:")
    print("  docker-compose -f examples/docker-compose-with-constraints.yml up -d")
