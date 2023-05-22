# PageRank on wikiarticles dataset using PySpark and HDFS

## Pre-requisites
Wiki articles edge data needs to be present of the hdfs file system. The below
command can be used to copy the files/folder from local
```
hadoop fs -put -r /path/to/local/folder hdfs://<hdfs name node ip>:9000/path/to/dest/folder
```

## Instructions to run
Assuming spark is present in the same folder,
```
./spark-3.1.2-bin-hadoop3.2/bin/spark-submit --master spark://<spark master ip>:7077 task.py hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* hdfs://10.10.1.1:9000/path/to/output/folder <num_partitions>
```

OR

if spark bin is present in PATH,
```
spark-submit --master spark://<spark master ip>:7077 task.py hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* hdfs://10.10.1.1:9000/path/to/output/folder <num_partitions>
```

OR

we can also directly call `run.sh <spark master url> <hdfs source path> <hdfs output path> <num_partitions>`, assuming spark bin is present in PATH.

## Results
With 3 Spark Worker Nodes, each with 1cpu (5 cores) and 16GB of RAM, the below
job completion times were observed for various partition sizes
```
50 - 42mins
150 - 31mins
300 -
```

Both `ranks` and `edges` RDDs are partitioned using the same number to see if there
is any performance gain compared to `task1` (where spark defaulted to 89 partitions).   

The input data being read is about 29.6GB and the output being written back to
hdfs is about 1GB.
