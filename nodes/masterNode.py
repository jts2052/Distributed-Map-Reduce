import multiprocessing as mp
from mapReduceLib.nodes.mapperNode import MapperNode
from mapReduceLib.nodes.reducerNode import ReducerNode
import socket as sock
import os
import sys
import time
import threading
import logging
import ast
from queue import Queue
from datetime import datetime

import config

class MasterNode:
    def __init__(self, num_mappers, num_reducers):
        self.num_mappers = num_mappers
        self.master_port = config.network["MASTER_PORT"]
        self.mapper_port = config.network["MAP_STARTING_PORT"]
        self.reducer_port = config.network["REDUCE_STARTING_PORT"]
        self.current_mapper_id = 0
        self.current_reducer_id = 0
        self.map_ids = []
        self.reduce_ids = []
        self.map_ports = {}  # {mapper_id: port}
        self.num_reducers = num_reducers
        self.reduce_ports = {}  # {reducer_id: port}
        self.current_cluster_id = 0
        self.cluster_ids = []
        self.map_processes = []
        self.reduce_processes = []
        self.data_slices = []
        self.received_messages = Queue()
        self.map_fn = None
        self.reduce_fn = None
        self.logger = logging.getLogger(__name__)
        self.log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.log_directory = "./mr_logging/" 

        self.logger.info(f"Checking if logging directory exists: {self.log_directory}")
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger.info(f"Logging directory exists or created successfully.")

    def configure_logging(self):
        log_file = os.path.join(self.log_directory, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
        self.logger.info(f"Creating log file: {log_file}")
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(self.log_format)
        
        # Remove all handlers associated with the logger object.
        for handler in self.logger.handlers:
            self.logger.removeHandler(handler)
        
        # Add our file_handler to the logger
        self.logger.addHandler(file_handler)
        self.logger.info(f"Log file created successfully.")

    def init_cluster(self, input_data, map_fn, reduce_fn, output_file):
        """
        Initialize a cluster with mappers and reducers

        Parameters:
        - input_data: Data to be processed
        - map_fn: Map function
        - reduce_fn: Reduce function
        - output_file: File to write output to
        """
        self.map_fn = map_fn
        self.reduce_fn = reduce_fn
        self.output_file = output_file

        self.configure_logging()

        input_data = self.handle_input_data(input_data)

        self.data_size_per_mapper = len(input_data) // self.num_mappers
        self.logger.info(f"Data size per mapper: {self.data_size_per_mapper}")
        self.data_remainder = len(input_data) % self.num_mappers

        self.init_mappers(input_data)

        self.current_cluster_id += 1
        self.cluster_ids.append(self.current_cluster_id)

        self.logger.info("Cluster initialized")
        return self.current_cluster_id

    def init_mappers(self, input_data):
        """
        Initialize mappers with input data
        - Creates slices of input data for each mapper
        - Initializes mapper processes
        - Processes stored in self.map_processes
        - Processes idle until started
        """
        start_index = 0
        for i in range(self.num_mappers):
            end_index = start_index + self.data_size_per_mapper
            if i < self.data_remainder:
                end_index += 1
            data_slice = input_data[start_index:end_index]
            self.data_slices.append(data_slice)
            start_index = end_index

            mapper = MapperNode(
                i,
                self.mapper_port,
                self.master_port,
                self.map_fn,
                receive_size=config.specs["MAPPER"]["MAX_RECEIVE_SIZE"],
            )
            mapper_process = mp.Process(target=mapper.run)
            self.map_processes.append(mapper_process)
            self.map_ids.append(self.current_mapper_id)
            self.current_mapper_id += 1
            self.map_ports[i] = self.mapper_port
            self.mapper_port += 1

    def handle_input_data(self, input_data):
        """
        Determines how input data is formatted
        """
        if type(input_data) == list:
            input_data = [(x, data) for x, data in enumerate(input_data)]
        elif type(input_data) == str and os.path.exists(input_data):
            with open(input_data, "r", encoding="utf-8") as file:
                input_data = file.read()
        return input_data

    def stop_cluster(self):
        """
        Stop cluster
        """
        for cluster_id in self.cluster_ids:
            self.send_to_all_mappers(f"{cluster_id}:STOP")
            self.send_to_all_reducers(f"{cluster_id}:STOP")

    def run_mapreduce(self):
        """
        Starts the map-reduce process
        1. Starts mapper processes
        2. Sends data to mappers
        3. Receives map output
        4. Starts reducer processes
        4. Sends data to reducers
        4. Receives reduce output
        """
        for i in range(self.num_mappers):
            self.logger.info(f"Starting mapper {i}")
            self.map_processes[i].start()

        if self.is_multidimensional(self.data_slices):
            threads = [
                threading.Thread(target=self.send_to_mapper, args=(x, "MULTI"))
                for x in range(self.num_mappers)
            ]
        else:
            threads = [
                threading.Thread(target=self.send_to_mapper, args=(x, "START"))
                for x in range(self.num_mappers)
            ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        time.sleep(0.1)  # Let nodes start up

        if self.is_multidimensional(self.data_slices):
            threads = [
                threading.Thread(
                    target=self.send_to_mapper, args=(x, self.data_slices[x])
                )
                for x in range(self.num_mappers)
            ]
        else:
            threads = [
                threading.Thread(
                    target=self.send_to_mapper, args=(x, self.data_slices[x])
                )
                for x in range(self.num_mappers)
            ]
        for thread in threads:
            thread.start()

        self.receive_map_output()

        self.start_reducers()

    def receive_map_output(self):
        """
        Receive output from mappers
        1. Create a socket to listen for connections
        2. Accept connections from mappers
        3. Handle connections in separate threads
        4. Close connections when finished
        5. Close socket when finished
        """
        def handle_connection(conn):
            try:
                while True:
                    data = conn.recv(config.specs["MAPPER"]["MAX_RECEIVE_SIZE"])
                    if not data:
                        break
                    self.received_messages.put(data.decode())
            except Exception as e:
                self.logger.error("Error handling connection:", e)
            finally:
                conn.close()

        s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
        s.bind((config.network["SERVER_NAME"], self.master_port))
        s.listen()

        threads = []
        try:
            for _ in range(self.num_mappers):
                conn, addr = s.accept()
                self.logger.info(f"Connection from mapper {addr} in master node")
                thread = threading.Thread(target=handle_connection, args=(conn,))
                thread.start()
                threads.append(thread)
        except Exception as e:
            self.logger.error("Error accepting connection:", e)
        finally:
            for thread in threads:
                thread.join()
            s.close()

    def receive_reduce_output(self):
        """
        Receive output from reducers
        1. Create a socket to listen for connections
        2. Accept connections from reducers
        3. Handle connections in separate threads
        4. Close connections when finished
        5. Close socket when finished
        """
        def handle_connection(conn):
            try:
                while True:
                    data = conn.recv(config.specs["REDUCER"]["MAX_RECEIVE_SIZE"])
                    if not data:
                        break
                    self.received_messages.put(data.decode())
            except Exception as e:
                self.logger.error("Error handling connection:", e)
            finally:
                conn.close()

        s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
        s.bind((config.network["SERVER_NAME"], self.master_port))
        s.listen()

        threads = []
        try:
            for _ in range(self.num_reducers):
                conn, addr = s.accept()
                self.logger.info(f"Connection from reducer {addr} in master node")
                thread = threading.Thread(target=handle_connection, args=(conn,))
                thread.start()
                threads.append(thread)
        except Exception as e:
            self.logger.error("Error accepting connection:", e)
        finally:
            for thread in threads:
                thread.join()
            s.close()

    def start_reducers(self):
        """
        Start reducer processes
        1. Create reducer processes
        2. Start reducer processes
        3. Send data to reducers
        4. Send DONE message to reducers
        5. Receive reduce output
        6. Output results
        """
        for i in range(self.num_reducers):
            reducer = ReducerNode(
                i,
                self.reducer_port,
                self.master_port,
                self.reduce_fn,
                receive_size=config.specs["REDUCER"]["MAX_RECEIVE_SIZE"],
            )
            reducer_process = mp.Process(target=reducer.run)
            self.reduce_processes.append(reducer_process)
            self.reduce_ids.append(self.current_reducer_id)
            self.current_reducer_id += 1
            self.reduce_ports[i] = self.reducer_port
            self.reducer_port += 1

        for process in self.reduce_processes:
            process.start()

        threads = [
            threading.Thread(target=self.send_to_reducer, args=(x, "START"))
            for x in range(self.num_reducers)
        ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        time.sleep(0.1)  # Let nodes start up

        # Send data to reducers
        # - Get data from received_messages queue
        # - Combine data from all mappers
        # - Use a hash function to determine which reducer to send data to
        # - Send data to reducers
        # - When all data has been sent, send a DONE message to each reducer
        all_data = [self.received_messages.get() for _ in range(self.num_mappers)]

        all_data = [data.split(":")[1] for data in all_data if ":" in data]
        all_data = [ast.literal_eval(data) for data in all_data]
        all_data = self.combine_results(all_data)

        # Use hash function to determine which reducer to send data to
        # - Hash function: key % num_reducers
        # - Each reducer will get the same key
        def hash_fn(key):
            return hash(key) % self.num_reducers

        data_per_reducer = len(all_data) // self.num_reducers
        remainder = len(all_data) % self.num_reducers
        start_index = 0
        for i in range(self.num_reducers):
            end_index = start_index + data_per_reducer
            if i < remainder:
                end_index += 1
            data_slice = all_data[start_index:end_index]
            start_index = end_index

            self.send_to_reducer(i, str(data_slice))

        self.send_to_all_reducers("DONE")

        self.receive_reduce_output()

        self.output_results()

    def stop_reducers(self):
        for reducer_id in self.reduce_ids:
            self.send_to_reducer(reducer_id, "STOP")

    def stop_mappers(self):
        for mapper_id in self.map_ids:
            self.send_to_mapper(mapper_id, "STOP")

    def stop_all(self):
        self.stop_reducers()
        self.stop_mappers()

    def output_results(self):
        with open(self.output_file, "w") as file:
            while not self.received_messages.empty():
                data = self.received_messages.get()
                file.write(f"{data}\n")

    """
        combine_results:
        Combine results from mappers
        - Data is in the form of [(key, value), ...]
        - We want [(key, [value1, value2, ...]), ...
    """

    def combine_results(self, data):
        combined_data = []
        for layer in data:
            for key, value in layer:
                if key not in [x[0] for x in combined_data]:
                    combined_data.append((key, [value]))
                else:
                    index = [x[0] for x in combined_data].index(key)
                    combined_data[index][1].append(value)
        return combined_data

    def send_to_mapper(self, mapper_id, message):
        self.send_to_port(
            self.map_ports[mapper_id], f"{self.map_ports[mapper_id]}:{message}"
        )

    def send_to_reducer(self, reducer_id, message):
        self.send_to_port(
            self.reduce_ports[reducer_id], f"{self.reduce_ports[reducer_id]}:{message}"
        )

    def send_to_all_mappers(self, message):
        for id in self.map_ids:
            self.send_to_mapper(id, message)

    def send_to_all_reducers(self, message):
        for id in self.reduce_ids:
            self.send_to_reducer(id, message)

    def send_to_port(self, port, message):
        try:
            self.logger.info(f"Sending message to port {port}")
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.connect((config.network["SERVER_NAME"], port))
            s.sendall(message.encode())
        except Exception as e:
            self.logger.error(f"Error sending message to port {port}: {e}")
        finally:
            s.close()

    def is_multidimensional(self, lst):
        if isinstance(lst, list):
            if any(isinstance(sublist, list) for sublist in lst):
                return True
        return False
