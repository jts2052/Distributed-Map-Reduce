# Distributed MapReduce Program

## Overview 
The MapReduce system consists of a master node that coordinates the execution of mapper and reducer nodes. 
It allows for parallel processing of large datasets using a map-reduce paradigm.

## Usage
Download and place mapReduceLib into the root directory of wherever you want to use it.

#### Initializing MapReduce System
```python
from mapReduceLib import mapReduce

# Initialize MapReduce instance
mr = mapReduce(num_mappers=6, num_reducers=6)
```

### Initializing Cluster
```python
# Initialize cluster with input data, map function, reduce function, and output location
# - input_data (string/list/file_path): The list of input data to be processed.
# - map_fn (function): The map function to be used.
# - red_fn (function): The reduce function to be used.
# - output_location (string): The location to save the output.

cluster_id = mr.init_cluster(input_data, map_fn, red_fun, output_location)
```

### Running MapReduce Process
```python
mr.run()
```
