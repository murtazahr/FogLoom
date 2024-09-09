import json
import os
from typing import Dict, List, Any
from abc import ABC, abstractmethod
import couchdb
import time
import random
import traceback
import logging
from collections import defaultdict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

COUCHDB_URL = f"http://{os.getenv('COUCHDB_USER')}:{os.getenv('COUCHDB_PASSWORD')}@{os.getenv('COUCHDB_HOST', 'couch-db-0:5984')}"
COUCHDB_DB = 'resource_registry'


class BaseScheduler(ABC):
    @abstractmethod
    def __init__(self, dependency_graph: Dict[str, Any], app_requirements: Dict[str, Dict[str, int]],
                 db_config: Dict[str, str]):
        """
        Initialize the scheduler with the necessary configuration and data.

        :param dependency_graph: A dictionary representing the workflow dependency graph
        :param app_requirements: A dictionary mapping application IDs to their resource requirements
        :param db_config: A dictionary containing database configuration (e.g., URL, name)
        """
        pass

    @abstractmethod
    def schedule(self, iot_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Schedule the applications based on the input IoT data.

        :param iot_data: A list of dictionaries containing IoT data to be processed
        :return: A dictionary mapping node IDs to lists of scheduled application IDs
        """
        pass


class LCDWRRScheduler(BaseScheduler):
    def __init__(self, dependency_graph: Dict[str, Any], app_requirements: Dict[str, Dict[str, int]],
                 db_config: Dict[str, str]):
        logging.info("Initializing LCDWRRScheduler")
        self.dependency_graph = dependency_graph
        self.app_requirements = app_requirements

        # CouchDB connection
        logging.debug(f"Connecting to CouchDB with config: {db_config}")
        self.couch = couchdb.Server(db_config['url'])
        self.db = self.couch[db_config['name']]
        logging.info("Connected to CouchDB successfully")

    def calculate_load(self, node: Dict) -> float:
        cpu_load = node['resource_data']['cpu']['used_percent'] / 100
        memory_load = node['resource_data']['memory']['used_percent'] / 100
        return (cpu_load * 0.5) + (memory_load * 0.5)

    def get_latest_node_data(self):
        logging.info("Fetching latest node data from CouchDB")
        node_resources = {'rows': []}
        for row in self.db.view('_all_docs', include_docs=True):
            if row.id.startswith('resource_'):
                node_resources['rows'].append({'id': row.id, 'doc': row.doc})
        logging.debug(f"Fetched {len(node_resources['rows'])} node resources")
        return node_resources

    def select_node(self, app_id: str, node_resources: Dict[str, List[Dict]]) -> str:
        logging.debug(f"Selecting node for app: {app_id}")
        eligible_nodes = []
        for node in node_resources['rows']:
            if (self.app_requirements[app_id]['cpu'] <= node['doc']['resource_data']['cpu']['total'] and
                    self.app_requirements[app_id]['memory'] <= node['doc']['resource_data']['memory']['total']):
                eligible_nodes.append(node)

        if not eligible_nodes:
            logging.error(f"No eligible nodes found for application {app_id}")
            raise ValueError(f"No eligible nodes found for application {app_id}")

        least_loaded_nodes = sorted(eligible_nodes, key=lambda x: self.calculate_load(x['doc']))[:3]  # Top 3 least loaded nodes
        weights = [1 / (self.calculate_load(node['doc']) + 0.1) for node in least_loaded_nodes]  # Inverse load as weight
        selected_node = random.choices(least_loaded_nodes, weights=weights)[0]

        logging.debug(f"Selected node {selected_node['id']} for app {app_id}")
        return selected_node['id']

    def topological_sort(self):
        # Create a graph representation
        graph = defaultdict(list)
        in_degree = {node: 0 for node in self.dependency_graph['nodes']}

        for edge in self.dependency_graph['edges']:
            source, target = edge['source'], edge['target']
            graph[source].append(target)
            in_degree[target] += 1

        # Initialize queue with nodes having no dependencies
        queue = [node for node in in_degree if in_degree[node] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(self.dependency_graph['nodes']):
            raise ValueError("Graph has a cycle")

        return result

    def schedule(self, iot_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        logging.info("Starting scheduling process")
        node_resources = self.get_latest_node_data()
        schedule = {node['id']: [] for node in node_resources['rows']}

        # Get the topologically sorted order of apps
        sorted_apps = self.topological_sort()
        logging.debug(f"Topologically sorted apps: {sorted_apps}")

        for app_id in sorted_apps:
            logging.debug(f"Scheduling app: {app_id}")
            selected_node = self.select_node(app_id, node_resources)
            schedule[selected_node].append(app_id)
            logging.debug(f"Scheduled {app_id} on node {selected_node}")
            # Update the node's load in our local data
            for node in node_resources['rows']:
                if node['id'] == selected_node:
                    node['doc']['resource_data']['cpu']['used_percent'] += self.app_requirements[app_id]['cpu']
                    node['doc']['resource_data']['memory']['used_percent'] += self.app_requirements[app_id]['memory']
                    break

        logging.info("Scheduling process completed")
        return schedule


def create_scheduler(scheduler_type: str, dependency_graph: Dict, app_requirements: Dict, db_config: Dict) -> (
        BaseScheduler):
    if scheduler_type == "lcdwrr":
        return LCDWRRScheduler(dependency_graph, app_requirements, db_config)
    else:
        raise ValueError(f"Unknown scheduler type: {scheduler_type}")


def main():
    try:
        logging.info("Starting the scheduler")

        # Load input data
        try:
            with open('dependency_graph.json', 'r') as f:
                dependency_graph = json.load(f)
            logging.debug("Loaded dependency_graph.json")

            with open('app_requirements.json', 'r') as f:
                app_requirements = json.load(f)
            logging.debug("Loaded app_requirements.json")

            with open('iot_data.json', 'r') as f:
                iot_data = json.load(f)
            logging.debug("Loaded iot_data.json")
        except FileNotFoundError as e:
            logging.error(f"Error: Required input file not found. {str(e)}")
            return
        except json.JSONDecodeError as e:
            logging.error(f"Error: Invalid JSON in input file. {str(e)}")
            return

        db_config = {
            "url": "http://fogbus:mwg478jR04vAonMu2QnFYF3sVyVKUujYrGrzVsrq3I@couchdb-0.default.svc.cluster.local:5984/",
            "name": "resource_registry"
        }
        logging.debug(f"DB Config: {db_config}")

        # Initialize scheduler
        logging.info("Initializing scheduler")
        scheduler = create_scheduler("lcdwrr", dependency_graph, app_requirements, db_config)
        logging.info("Scheduler initialized successfully")

        # Main loop
        try:
            while True:
                logging.info("Running scheduler")
                # Run the scheduler
                result = scheduler.schedule(iot_data['iot_data'])
                print(json.dumps(result, indent=2))
                logging.info("Scheduler run completed")

                # Simulate receiving new IoT data every 30 seconds
                time.sleep(30)
        except KeyboardInterrupt:
            logging.info("Received KeyboardInterrupt. Shutting down...")
    except Exception as e:
        logging.error("An unexpected error occurred:")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
