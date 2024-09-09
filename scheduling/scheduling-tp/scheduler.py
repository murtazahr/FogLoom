import json
from typing import Dict, List, Any
from abc import ABC, abstractmethod
import couchdb
import random
import logging

logger = logging.getLogger(__name__)


class BaseScheduler(ABC):
    @abstractmethod
    def __init__(self, dependency_graph: Dict[str, Any], app_requirements: Dict[str, Dict[str, int]],
                 db_config: Dict[str, str]):
        pass

    @abstractmethod
    def schedule(self, iot_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
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

    def topological_sort(self):
        # Create a graph representation
        graph = {node: self.dependency_graph['nodes'][node].get('next', []) for node in self.dependency_graph['nodes']}
        in_degree = {node: 0 for node in graph}
        for node in graph:
            for neighbor in graph[node]:
                in_degree[neighbor] += 1

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

    def select_node(self, app_id: str, node_resources: Dict[str, List[Dict]]) -> str:
        eligible_nodes = []
        for node in node_resources['rows']:
            if (self.app_requirements[app_id]['cpu'] <= node['doc']['resource_data']['cpu']['total'] and
                    self.app_requirements[app_id]['memory'] <= node['doc']['resource_data']['memory']['total']):
                eligible_nodes.append(node)

        if not eligible_nodes:
            raise ValueError(f"No eligible nodes found for application {app_id}")

        least_loaded_nodes = sorted(eligible_nodes, key=lambda x: self.calculate_load(x['doc']))[
                             :3]  # Top 3 least loaded nodes
        weights = [1 / (self.calculate_load(node['doc']) + 0.1) for node in
                   least_loaded_nodes]  # Inverse load as weight
        selected_node = random.choices(least_loaded_nodes, weights=weights)[0]

        return selected_node['id']

    def schedule(self, iot_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        node_resources = self.get_latest_node_data()
        schedule = {node['id']: [] for node in node_resources['rows']}

        # Get the topologically sorted order of apps
        sorted_apps = self.topological_sort()
        logger.debug(f"Topologically sorted apps: {sorted_apps}")

        for app_id in sorted_apps:
            logger.debug(f"Scheduling app: {app_id}")
            selected_node = self.select_node(app_id, node_resources)
            schedule[selected_node].append(app_id)
            logger.debug(f"Scheduled {app_id} on node {selected_node}")
            # Update the node's load in our local data
            for node in node_resources['rows']:
                if node['id'] == selected_node:
                    node['doc']['resource_data']['cpu']['used_percent'] += self.app_requirements[app_id]['cpu']
                    node['doc']['resource_data']['memory']['used_percent'] += self.app_requirements[app_id]['memory']
                    break

        return schedule


def create_scheduler(scheduler_type: str, dependency_graph: Dict, app_requirements: Dict,
                     db_config: Dict) -> BaseScheduler:
    if scheduler_type == "lcdwrr":
        return LCDWRRScheduler(dependency_graph, app_requirements, db_config)
    else:
        raise ValueError(f"Unknown scheduler type: {scheduler_type}")
