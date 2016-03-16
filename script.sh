#!/bin/sh


#export HADOOP_CLASSPATH=/home/bsugiart/hadoopjar/json-mapreduce-1.0.jar
javac -cp \* PowerDays.java
jar cvf powerdays.jar *.class	
hadoop fs -rm -r /user/bsugiart/test
rm -r output
hadoop jar powerdays.jar PowerDays
hadoop fs -cat test/output/part-r-00000 


