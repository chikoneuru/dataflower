import os
import logging

import docker

logger = logging.getLogger(__name__)


class ContainerManager:
    def __init__(self):
        self.client = docker.from_env()
        self.workers = []
        self.discover_workers()

    def get_workers(self):
        """Returns the list of discovered worker nodes."""
        return self.workers

    def discover_workers(self):
        """
        Finds running worker containers managed by docker-compose and stores their info.
        """
        self.discover_worker_containers()

    def discover_worker_containers(self):
        """
        Discover Ditto worker containers and return a list of dicts with keys:
          - container_id, name, worker_id, port, status
        This is used by the Ditto orchestrator.
        """
        logger.debug("Discovering worker containers...")
        discovered = []
        try:
            containers = self.client.containers.list(filters={'label': 'dataflower-type=generic-worker'})
            for c in containers:
                # Prefer NetworkSettings -> Ports for host port extraction
                port = None
                try:
                    ports = c.attrs.get('NetworkSettings', {}).get('Ports', {}) or {}
                    # Look for any mapped port, typically 5000/tcp
                    for _, host_bindings in ports.items():
                        if host_bindings and isinstance(host_bindings, list) and 'HostPort' in host_bindings[0]:
                            port = int(host_bindings[0]['HostPort'])
                            break
                    if port is None:
                        # Fallback to HostConfig.PortBindings
                        pb = c.attrs.get('HostConfig', {}).get('PortBindings', {}) or {}
                        if '5000/tcp' in pb and pb['5000/tcp']:
                            port = int(pb['5000/tcp'][0]['HostPort'])
                except Exception:
                    # Keep port as None if extraction fails
                    pass

                discovered.append({
                    'container_id': c.id,
                    'name': c.name,
                    'worker_id': c.name,
                    'port': port,
                    'status': getattr(c, 'status', 'unknown'),
                })
            
            # Store discovered workers in instance variable
            self.workers = discovered
            logger.debug(f"Successfully discovered and registered {len(discovered)} workers")
        except docker.errors.DockerException as e:
            logger.error(f"Error connecting to Docker daemon: {e}")
            logger.error("Please ensure Docker is running.")
        return discovered


    def list_functions(self, workflow_name):
        # This function is part of the old model and is no longer accurate
        # in the worker architecture. It is kept for compatibility but will be deprecated.
        print("Warning: list_functions is deprecated in worker-based architecture.")
        return []

    def run_function(self, image_name, container_name, env_vars=None):
        # This function is part of the old model and is no longer used for
        # invoking functions in the worker architecture.
        print("Warning: run_function is deprecated in worker-based architecture.")
        pass

    def remove_function(self, container_name):
        # This function is part of the old model.
        print("Warning: remove_function is deprecated in worker-based architecture.")
        pass
