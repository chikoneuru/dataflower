#!/usr/bin/env python3
"""
Real-time Resource Integration Demo

This script demonstrates how the updated cost model, placement, and routing 
components now access real-time resource information from the container manager.
"""

import logging
import time

from provider.function_container_manager import FunctionContainerManager
from scheduler.ours.placement import PlacementRequest, PlacementStrategy
from scheduler.ours.resource_integration import create_integrated_cost_models
from scheduler.ours.routing import RoutingRequest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demonstrate_real_time_resources():
    """Demonstrate real-time resource integration"""
    logger.info("=== Real-time Resource Integration Demo ===")
    
    # Initialize container manager
    logger.info("Initializing container manager...")
    container_manager = FunctionContainerManager()
    
    # Create integrated cost models and components
    logger.info("Creating integrated cost models...")
    resource_integration = create_integrated_cost_models(container_manager)
    
    # Create placement and routing components with real-time resource access
    placement_component = resource_integration.create_placement_component()
    routing_component = resource_integration.create_routing_component()
    
    # Display current cluster resources
    logger.info("\n=== Current Cluster Resources ===")
    cluster_resources = resource_integration.get_real_time_cluster_resources()
    
    for node_id, resources in cluster_resources.items():
        logger.info(f"Node {node_id}:")
        logger.info(f"  CPU: {resources['available_cpu']:.1f}/{resources['total_cpu']:.1f} "
                   f"({resources['cpu_utilization']:.1%} utilized)")
        logger.info(f"  Memory: {resources['available_memory']:.1f}GB/{resources['total_memory']:.1f}GB "
                   f"({resources['memory_utilization']:.1%} utilized)")
        logger.info(f"  Running containers: {resources['running_containers']}")
    
    # Demonstrate placement decision with real-time resources
    logger.info("\n=== Placement Decision Demo ===")
    
    # Create a placement request
    placement_request = PlacementRequest(
        function_name="recognizer__adult",
        cpu_requirement=0.5,
        memory_requirement=512,  # MB
        priority=1
    )
    
    # Get available nodes from container manager
    available_nodes = container_manager.get_nodes()
    
    if available_nodes:
        logger.info(f"Finding placement for {placement_request.function_name}...")
        
        # Try different placement strategies
        strategies = [
            PlacementStrategy.RESOURCE_AWARE,
            PlacementStrategy.INITIAL_GREEDY,
            PlacementStrategy.TARGETED_LNS
        ]
        
        for strategy in strategies:
            logger.info(f"\nUsing {strategy.value} strategy:")
            
            result = placement_component.find_best_placement(
                placement_request, 
                available_nodes, 
                container_manager.function_containers,
                strategy
            )
            
            if result:
                logger.info(f"  Selected node: {result.target_node}")
                logger.info(f"  Score: {result.score:.3f}")
                logger.info(f"  Reasoning: {result.reasoning}")
            else:
                logger.info("  No suitable node found")
    
    # Demonstrate routing decision with real-time resources
    logger.info("\n=== Routing Decision Demo ===")
    
    # Get existing containers for a function
    function_name = "recognizer__adult"
    existing_containers = container_manager.get_function_containers(function_name)
    
    if existing_containers:
        logger.info(f"Routing request for {function_name}...")
        
        routing_request = RoutingRequest(
            function_name=function_name,
            request_id="demo_request_001",
            latency_priority=True
        )
        
        routing_result = routing_component.route_request(
            routing_request,
            existing_containers
        )
        
        if routing_result:
            logger.info(f"  Selected container: {routing_result.target_container.container_id}")
            logger.info(f"  Node: {routing_result.target_container.node_id}")
            logger.info(f"  Load score: {routing_result.load_score:.3f}")
            logger.info(f"  Reasoning: {routing_result.reasoning}")
        else:
            logger.info("  No suitable container found")
    else:
        logger.info(f"No existing containers found for {function_name}")
    
    # Demonstrate cost estimation with real-time resources
    logger.info("\n=== Cost Estimation Demo ===")
    
    # Create a task profile
    task_profile = resource_integration.create_task_profile(
        function_name="recognizer__adult",
        task_id="demo_task_001",
        memory_requirement=512,  # MB
        estimated_compute_time=0.3,
        cpu_intensity=0.7
    )
    
    # Estimate costs on different nodes
    for node_id, resources in cluster_resources.items():
        logger.info(f"\nCost estimation for node {node_id}:")
        
        costs = resource_integration.estimate_placement_cost(
            task_profile, 
            node_id
        )
        
        if 'error' not in costs:
            logger.info(f"  Compute cost: {costs['compute']:.3f}s")
            logger.info(f"  Cold start cost: {costs['cold_start']:.3f}s")
            logger.info(f"  Interference cost: {costs['interference']:.3f}s")
            logger.info(f"  Total cost: {costs['total']:.3f}s")
        else:
            logger.info(f"  Error: {costs['error']}")
    
    # Show resource utilization monitoring
    logger.info("\n=== Resource Monitoring Demo ===")
    
    for i in range(3):
        logger.info(f"\nResource check #{i+1}:")
        
        # Update all resources
        resource_integration.update_all_node_resources()
        
        # Get updated cluster status
        cluster_status = container_manager.get_cluster_status()
        
        logger.info(f"Total nodes: {cluster_status['total_nodes']}")
        logger.info(f"Running containers: {cluster_status['running_containers']}")
        
        # Check capacity for a new container (CPU cores, Memory GB)
        for node_id in cluster_resources.keys():
            can_place = resource_integration.check_node_capacity(node_id, 0.5, 0.5)  # 0.5 CPU, 0.5GB memory
            logger.info(f"Node {node_id} can place new container: {can_place}")
        
        if i < 2:  # Don't sleep on the last iteration
            time.sleep(2)
    
    logger.info("\n=== Demo Complete ===")

if __name__ == "__main__":
    try:
        demonstrate_real_time_resources()
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed with error: {e}", exc_info=True)
