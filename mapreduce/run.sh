#!/bin/bash

hdfs dfs -rm -r -f /data/out

#export HADOOP_CLASSPATH="$(hadoop classpath):$(hbase classpath)"
export HADOOP_CLASSPATH="$(hbase classpath)"
#export LIBJARS=hadoop-lzo-0.4.20-SNAPSHOT.jar
#hadoop jar a.jar hi.mr.Aggregate -libjars hadoop-lzo-0.4.20-SNAPSHOT.jar   /data/bitcoin_tmp4/ /data/out
#hadoop jar a.jar hi.mr.Aggregate -libjars ${LIBJARS}   /data/bitcoin_tmp4/ /data/out
hadoop jar a.jar hi.mr.Aggregate /data/bitcoin_tmp3/ /data/out


hdfs dfs -cat /data/out/part-r-00000

