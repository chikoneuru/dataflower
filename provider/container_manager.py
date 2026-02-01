import docker
import os

class ContainerManager:
    def __init__(self):
        self.client = docker.from_env()
        self.workers = []
        self.discover_workers()

    def discover_workers(self):
        """
        Finds running worker containers managed by docker-compose and stores their info.
        """
        print("  - Discovering worker containers...")
        self.workers = []
        try:
            # Find containers with the 'dataflower-role=worker' label
            worker_containers = self.client.containers.list(filters={'label': 'dataflower-role=worker'})
            for container in worker_containers:
                host_port = None
                try:
                    # Ports are exposed as '5000/tcp' in the container attributes
                    ports = container.attrs['HostConfig']['PortBindings']
                    if '5000/tcp' in ports and ports['5000/tcp'] is not None:
                        host_port = ports['5000/tcp'][0]['HostPort']
                    else:
                         print(f"    - Warning: Worker {container.name} has no host port binding for 5000/tcp.")
                         continue
                except (KeyError, IndexError, TypeError) as e:
                    print(f"    - Warning: Could not determine host port for worker {container.name}. Error: {e}")
                    continue
                
                if host_port:
                    worker_info = {
                        'id': container.id,
                        'name': container.name,
                        'container': container,
                        'host_port': host_port
                    }
                    self.workers.append(worker_info)
            
            print(f"  - Successfully discovered and registered {len(self.workers)} workers.")
        except docker.errors.DockerException as e:
            print(f"  - Error connecting to Docker daemon: {e}")
            print("    Please ensure Docker is running.")

    def get_workers(self):
        """Returns the list of discovered worker nodes."""
        return self.workers

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
