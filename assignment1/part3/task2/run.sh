#!/bin/bash

if [ "$#" -ne 4 ]; then
	echo 'Explected 4 parameters. Run "./run.sh spark://<spark master ip>:7077 hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* hdfs://10.10.1.1:9000/path/to/output/folder <num_partitions>"'
	exit 1
fi

if ! [[ "$1" =~ spark://* ]] ; then
	echo 'Argument 1 should be of the form spark://<spark master ip>:7077'
	exit 1
fi

if ! [[ "$2" =~ hdfs://* ]] || ! [[ "$3" =~ hdfs://* ]]  ; then
	echo 'Argument 2 and 3 should be of the form hdfs://<hdfs name node ip>:9000/path/to/folder'
	exit 1
fi

spark-submit --master "$1" task.py "$2" "$3" "$4"
