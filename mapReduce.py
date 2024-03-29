import sys
from typing import Union
from typing import Type

from mapReduceLib.nodes import masterNode

class MapReduce:
    def __init__(self, num_mappers, num_reducers):
        """
        Initializes a MapReduce system.

        Parameters:
        - num_mappers (int): The number of mapper nodes to create.
        - num_reducers (int): The number of reducer nodes to create.
        """
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        pass

    def init_cluster(self, input_data: Union[str, list], map_fn, red_fn, output_location: str) -> int:
        """
        Initializes the cluster with the given input data, map and reduce functions, and output location.

        Parameters:
        input_data (string/list/file_path): The list of input data to be processed.
        map_fn (function): The map function to be used.
        red_fn (function): The reduce function to be used.
        output_location (string): The location to save the output.

        Returns:
        - cluster_id (int): The ID of the cluster.
        """
        self.mn = masterNode.MasterNode(self.num_mappers, self.num_reducers)
        cluster_id = self.mn.init_cluster(input_data, map_fn, red_fn, output_location)
        return cluster_id

    def run(self):
        """
        Runs the map-reduce process.
        """
        self.mn.run_mapreduce()

    def stop_cluster(self, cluster_id):
        """
        Stops the cluster with the given ID.
        """
        self.mn.stop_cluster(cluster_id)
        self.mn = None

    def stop_mappers(self):
        """
        Stops all mappers in the cluster.
        """
        self.mn.stop_mappers()

    def stop_reducers(self):
        """
        Stops all reducers in the cluster.
        """
        self.mn.stop_reducers()