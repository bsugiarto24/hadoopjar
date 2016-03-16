#!/bin/sh


#export HADOOP_CLASSPATH=/home/bsugiart/hadoopjar/json-mapreduce-1.0.jar
javac -cp \* PowerComp.java
jar cvf powercomp.jar *.class	
hadoop fs -rm -r /user/bsugiart/test
rm -r output
hadoop jar powercomp.jar PowerComp
hadoop fs -cat test/output/part-r-00000 


