#!/bin/sh


#export HADOOP_CLASSPATH=/home/bsugiart/hadoopjar/json-mapreduce-1.0.jar
javac -cp \* PowerComp.java
jar cvf powercomp.jar *.class	
hadoop fs -rm -r /user/bsugiart/test
hadoop jar powercomp.jar PowerComp
hadoop fs -get test/output/part-r-00000 output.txt
head -20 output.txt

