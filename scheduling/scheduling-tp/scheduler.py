import json
import os
from typing import Dict, List, Any
from abc import ABC, abstractmethod
import couchdb
import time
import random

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
        self.dependency_graph = dependency_graph
        self.app_requirements = app_requirements

        # CouchDB connection
        self.couch = couchdb.Server(db_config['url'])
        self.db = self.couch[db_config['name']]

    def calculate_load(self, node: Dict) -> float:
        cpu_load = node['resource_data']['cpu']['used_percent'] / 100
        memory_load = node['resource_data']['memory']['used_percent'] / 100
        return (cpu_load * 0.5) + (memory_load * 0.5)

    def get_latest_node_data(self):
        node_resources = {'rows': []}
        for row in self.db.view('_all_docs', include_docs=True):
            if row.id.startswith('resource_'):
                node_resources['rows'].append({'id': row.id, 'doc': row.doc})
        return node_resources

    def select_node(self, app_id: str, node_resources: Dict[str, List[Dict]]) -> str:
        eligible_nodes = []
        for node in node_resources['rows']:
            if (self.app_requirements[app_id]['cpu'] <= node['doc']['resource_data']['cpu']['total'] and
                    self.app_requirements[app_id]['memory'] <= node['doc']['resource_data']['memory']['total']):
                eligible_nodes.append(node)

        if not eligible_nodes:
            raise ValueError(f"No eligible nodes found for application {app_id}")

        least_loaded_nodes = sorted(eligible_nodes, key=lambda x: self.calculate_load(x['doc']))[:3]  # Top 3 least loaded nodes
        weights = [1 / (self.calculate_load(node['doc']) + 0.1) for node in least_loaded_nodes]  # Inverse load as weight
        selected_node = random.choices(least_loaded_nodes, weights=weights)[0]

        return selected_node['id']

    def schedule(self, iot_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        node_resources = self.get_latest_node_data()
        schedule = {node['id']: [] for node in node_resources['rows']}

        for app_id in self.dependency_graph['nodes']:
            selected_node = self.select_node(app_id, node_resources)
            schedule[selected_node].append(app_id)
            # Update the node's load in our local data
            for node in node_resources['rows']:
                if node['id'] == selected_node:
                    node['doc']['resource_data']['cpu']['used_percent'] += self.app_requirements[app_id]['cpu']
                    node['doc']['resource_data']['memory']['used_percent'] += self.app_requirements[app_id]['memory']
                    break

        return schedule


def create_scheduler(scheduler_type: str, dependency_graph: Dict, app_requirements: Dict, db_config: Dict) -> (
        BaseScheduler):
    if scheduler_type == "lcdwrr":
        return LCDWRRScheduler(dependency_graph, app_requirements, db_config)
    else:
        raise ValueError(f"Unknown scheduler type: {scheduler_type}")


def run_scheduler(scheduler: BaseScheduler, iot_data: List[Dict[str, Any]]):
    try:
        result = scheduler.schedule(iot_data)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error occurred during scheduling: {str(e)}")


def main():
    # Load input data
    try:
        with open('dependency_graph.json', 'r') as f:
            dependency_graph = json.load(f)
        with open('app_requirements.json', 'r') as f:
            app_requirements = json.load(f)
        with open('iot_data.json', 'r') as f:
            iot_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error: Required input file not found. {str(e)}")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file. {str(e)}")
        return

    db_config = {
        "url": COUCHDB_URL,
        "name": COUCHDB_DB
    }

    try:
        # Initialize scheduler
        scheduler = create_scheduler("lcdwrr", dependency_graph, app_requirements, db_config)

        # Main loop
        try:
            while True:
                # Run the scheduler
                result = scheduler.schedule(iot_data['iot_data'])
                print(json.dumps(result, indent=2))

                # Simulate receiving new IoT data every 30 seconds
                time.sleep(30)
        except KeyboardInterrupt:
            print("Shutting down...")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


if __name__ == "__main__":
    main()
