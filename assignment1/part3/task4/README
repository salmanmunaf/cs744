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
./spark-3.1.2-bin-hadoop3.2/bin/spark-submit --master spark://<spark master ip>:7077 task.py hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* hdfs://10.10.1.1:9000/path/to/output/folder
```

OR

if spark bin is present in PATH,
```
spark-submit --master spark://<spark master ip>:7077 task.py hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* hdfs://10.10.1.1:9000/path/to/output/folder
```

OR

we can also directly call `run.sh <spark master url> <hdfs source path> <hdfs output path>`, assuming spark bin is present in PATH.

## Killing a worker node
```
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
```

We can use `jps` on the worker node to get the spark worker process id.
```
kill -9 <pid>
```
Results are observed for two cases, first when a worker is killed at 25% of
the application completion time and the other when the application reaches 75% of
the completion time (in a different run with all functioning workers).

## Results
3 Spark Worker Nodes, each with 1 cpu (5 cores) and 16GB of RAM. The input data
being read is about 29.6GB and the output being written back to hdfs is about 1GB.

Both `ranks` and `edges` are cached.

Compute time increases in both cases and is almost the same. It took 43 mins when
a worker was killed at 25% of the expected completion time (31mins), and took
around 44 mins when a worker was killed at 75% of the expected completion time.
