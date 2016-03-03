#!/bin/sh

export HADOOP_CLASSPATH=/home/bsugiart/hadoopjar/json-mapreduce-1.0.jar
make clean
javac -cp \* *.java
java -cp .:\* beFuddledGen
java -cp .:\* histogramSeq
hadoop fs -rm out.txt
hadoop fs -put out.txt
date
hadoop jar histogram.jar -libjars org.json-20120521.jar,json-mapreduce-1.0.jar out.txt test/output/
date