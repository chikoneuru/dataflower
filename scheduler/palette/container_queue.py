"""
Container Queue Manager for Palette Scheduler

Implements a queue system to prevent routing conflicts when multiple requests
target the same container simultaneously. This is especially important for
compute-intensive functions like mosaic and extract that can't handle
concurrent requests.
"""
import asyncio
import logging
import threading
import time
from typing import Dict, Optional, Any, Callable
from dataclasses import dataclass
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import uuid

from provider.function_container_manager import FunctionContainer


@dataclass
class QueuedRequest:
    """Represents a request queued for a specific container"""
    request_id: str
    function_name: str
    container_id: str
    body: Any
    color_hint: str
    callback: Callable[[Dict[str, Any]], None]
    queued_at: float
    priority: int = 0  # Higher priority = processed first


class ContainerQueueManager:
    """
    Manages queues for individual containers to prevent concurrent execution conflicts.
    
    Key features:
    - Per-container queues for functions that can't handle concurrency
    - Priority-based processing (higher priority first)
    - Automatic queue processing when containers become available
    - Configurable queue depth limits
    - Timeout handling for queued requests
    """
    
    def __init__(self, 
                 max_queue_depth: int = 10,
                 request_timeout: float = 300.0,  # 5 minutes
                 process_interval: float = 0.1):  # Check queues every 100ms
        self.logger = logging.getLogger(__name__)
        
        # Queue configuration
        self.max_queue_depth = max_queue_depth
        self.request_timeout = request_timeout
        self.process_interval = process_interval
        
        # Per-container state tracking
        self.container_queues: Dict[str, deque] = defaultdict(deque)
        self.container_busy: Dict[str, bool] = {}
        self.container_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        
        # Functions that require queuing (compute-intensive, can't handle concurrency)
        self.queued_functions = {
            'recognizer__mosaic',
            'recognizer__extract',
            'mosaic',  # Alternative naming
            'extract'   # Alternative naming
        }
        
        # Global state
        self._shutdown = False
        self._processing_thread = None
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="container-queue")
        
        # Statistics
        self.stats = {
            'total_queued': 0,
            'total_processed': 0,
            'total_timeouts': 0,
            'total_rejected': 0,
            'current_queue_sizes': {}
        }
        
        self.logger.info(f"ContainerQueueManager initialized with queued functions: {self.queued_functions}")
    
    def start(self):
        """Start the background queue processing thread"""
        if self._processing_thread is None or not self._processing_thread.is_alive():
            self._processing_thread = threading.Thread(
                target=self._process_queues_loop,
                name="container-queue-processor",
                daemon=True
            )
            self._processing_thread.start()
            self.logger.info("Container queue processing started")
    
    def stop(self):
        """Stop the queue processing and cleanup resources"""
        self._shutdown = True
        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5.0)
        self._executor.shutdown(wait=True)
        self.logger.info("Container queue processing stopped")
    
    def should_queue_for_function(self, function_name: str) -> bool:
        """Check if a function requires queuing"""
        return function_name in self.queued_functions
    
    def queue_request(self, 
                     request_id: str,
                     function_name: str,
                     container: FunctionContainer,
                     body: Any,
                     color_hint: str,
                     callback: Callable[[Dict[str, Any]], None],
                     priority: int = 0) -> Dict[str, Any]:
        """
        Queue a request for a specific container if it's busy.
        
        Returns:
            Dict with status indicating if request was queued or can proceed immediately
        """
        container_id = container.container_id
        
        # Check if we should queue this function
        if not self.should_queue_for_function(function_name):
            return {'status': 'proceed', 'message': 'Function does not require queuing'}
        
        # Get container lock
        container_lock = self.container_locks[container_id]
        
        with container_lock:
            # Check if container is busy
            if not self.container_busy.get(container_id, False):
                # Container is free, mark as busy and proceed
                self.container_busy[container_id] = True
                self.logger.debug(f"Container {container_id} is free, proceeding with request {request_id}")
                return {
                    'status': 'proceed',
                    'message': 'Container is free, proceeding immediately',
                    'container_marked_busy': True
                }
            
            # Container is busy, check queue depth
            queue = self.container_queues[container_id]
            if len(queue) >= self.max_queue_depth:
                self.stats['total_rejected'] += 1
                return {
                    'status': 'rejected',
                    'message': f'Queue full for container {container_id}',
                    'queue_size': len(queue)
                }
            
            # Queue the request
            queued_request = QueuedRequest(
                request_id=request_id,
                function_name=function_name,
                container_id=container_id,
                body=body,
                color_hint=color_hint,
                callback=callback,
                queued_at=time.time(),
                priority=priority
            )
            
            # Insert with priority (higher priority first)
            inserted = False
            for i, existing in enumerate(queue):
                if priority > existing.priority:
                    queue.insert(i, queued_request)
                    inserted = True
                    break
            
            if not inserted:
                queue.append(queued_request)
            
            self.stats['total_queued'] += 1
            self.stats['current_queue_sizes'][container_id] = len(queue)
            
            self.logger.info(
                f"Queued request {request_id} for {function_name} on container {container_id} "
                f"(queue size: {len(queue)}, priority: {priority})"
            )
            
            return {
                'status': 'queued',
                'message': f'Request queued for container {container_id}',
                'queue_position': len(queue),
                'queue_size': len(queue)
            }
    
    def release_container(self, container_id: str):
        """Mark a container as no longer busy"""
        container_lock = self.container_locks[container_id]
        
        with container_lock:
            self.container_busy[container_id] = False
            self.logger.debug(f"Container {container_id} released")
    
    def _process_queues_loop(self):
        """Background thread that processes queued requests"""
        self.logger.info("Queue processing loop started")
        
        while not self._shutdown:
            try:
                self._process_all_queues()
                time.sleep(self.process_interval)
            except Exception as e:
                self.logger.error(f"Error in queue processing loop: {e}")
                time.sleep(self.process_interval)
        
        self.logger.info("Queue processing loop stopped")
    
    def _process_all_queues(self):
        """Process all container queues"""
        current_time = time.time()
        
        for container_id in list(self.container_queues.keys()):
            self._process_container_queue(container_id, current_time)
    
    def _process_container_queue(self, container_id: str, current_time: float):
        """Process the queue for a specific container"""
        container_lock = self.container_locks[container_id]
        
        with container_lock:
            queue = self.container_queues[container_id]
            
            # Remove timed out requests
            self._remove_timed_out_requests(queue, current_time)
            
            # If container is busy, don't process queue
            if self.container_busy.get(container_id, False):
                return
            
            # If queue is empty, nothing to process
            if not queue:
                return
            
            # Get next request from queue
            queued_request = queue.popleft()
            self.stats['current_queue_sizes'][container_id] = len(queue)
            
            # Mark container as busy
            self.container_busy[container_id] = True
            
            # Process the request asynchronously
            self._executor.submit(
                self._process_queued_request,
                queued_request,
                container_id
            )
    
    def _remove_timed_out_requests(self, queue: deque, current_time: float):
        """Remove requests that have exceeded the timeout"""
        timeout_requests = []
        
        for request in queue:
            if current_time - request.queued_at > self.request_timeout:
                timeout_requests.append(request)
        
        for request in timeout_requests:
            queue.remove(request)
            self.stats['total_timeouts'] += 1
            self.logger.warning(
                f"Request {request.request_id} timed out after "
                f"{current_time - request.queued_at:.2f}s in queue"
            )
            
            # Call callback with timeout error
            try:
                request.callback({
                    'status': 'error',
                    'message': f'Request timed out in queue after {self.request_timeout}s',
                    'request_id': request.request_id
                })
            except Exception as e:
                self.logger.error(f"Error in timeout callback: {e}")
    
    def _process_queued_request(self, queued_request: QueuedRequest, container_id: str):
        """Process a single queued request"""
        try:
            self.logger.info(
                f"Processing queued request {queued_request.request_id} "
                f"for {queued_request.function_name} on container {container_id}"
            )
            
            # This is where the actual function invocation would happen
            # The callback will be called with the result
            # For now, we'll simulate this by calling the callback immediately
            # In the real implementation, this would invoke the actual function
            
            # Call the callback to indicate the request should now be processed
            queued_request.callback({
                'status': 'ready_to_process',
                'message': 'Request dequeued and ready for processing',
                'request_id': queued_request.request_id,
                'container_id': container_id
            })
            
            self.stats['total_processed'] += 1
            
        except Exception as e:
            self.logger.error(f"Error processing queued request {queued_request.request_id}: {e}")
            
            # Call callback with error
            try:
                queued_request.callback({
                    'status': 'error',
                    'message': f'Error processing queued request: {e}',
                    'request_id': queued_request.request_id
                })
            except Exception as callback_error:
                self.logger.error(f"Error in error callback: {callback_error}")
        
        finally:
            # Release the container after processing
            self.release_container(container_id)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            'stats': self.stats.copy(),
            'container_states': {
                container_id: {
                    'busy': self.container_busy.get(container_id, False),
                    'queue_size': len(self.container_queues[container_id])
                }
                for container_id in self.container_queues.keys()
            },
            'queued_functions': list(self.queued_functions),
            'max_queue_depth': self.max_queue_depth,
            'request_timeout': self.request_timeout
        }
    
    def add_queued_function(self, function_name: str):
        """Add a function to the list of functions that require queuing"""
        self.queued_functions.add(function_name)
        self.logger.info(f"Added {function_name} to queued functions")
    
    def remove_queued_function(self, function_name: str):
        """Remove a function from the list of functions that require queuing"""
        self.queued_functions.discard(function_name)
        self.logger.info(f"Removed {function_name} from queued functions")
