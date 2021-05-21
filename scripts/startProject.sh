#!/bin/bash

yes | $HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/bin/hdfs dfs -mkdir /user 
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/root 
$HADOOP_HOME/sbin/start-yarn.sh

if $($HADOOP_HOME/bin/hdfs dfs -test -d input); then $HADOOP_HOME/bin/hdfs dfs -rm -r input; fi

if $($HADOOP_HOME/bin/hdfs dfs -test -d output); then $HADOOP_HOME/bin/hdfs dfs -rm -r output; fi

$HADOOP_HOME/bin/hdfs dfs -mkdir /user/root/input
$HADOOP_HOME/bin/hdfs dfs -put /imported/input_data/* input

ans=""
while [ "$ans" = "" ];do
	echo "Input: s|m|h + Scale. Without Space!!!"
	read ans
done

$HADOOP_HOME/bin/yarn jar /imported/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output $ans

echo -e "\n\n######DEMONSTRATION OUTPUT DIRECTORY:"

$HADOOP_HOME/bin/hdfs dfs -ls output

echo -e "\n\n######DEMONSTRATION SEQUENCE FILE:"

$HADOOP_HOME/bin/hdfs dfs -cat output/part-r-00000

echo -e "\n\n######DEMONSTRATION CONTENT OF FILE:"

$HADOOP_HOME/bin/hdfs dfs -libjars /imported/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar -text output/part-r-00000