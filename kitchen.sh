#!/bin/sh


#export HADOOP_CLASSPATH=/home/bsugiart/hadoopjar/json-mapreduce-1.0.jar
javac -cp \* KitchenUse.java
jar cvf kitchenuse.jar *.class	
hadoop fs -rm -r /user/bsugiart/test
hadoop jar kitchenuse.jar KitchenUse
#hadoop fs -cat test/output/part-r-00000
hadoop fs -get test/output/part-r-00000 output.txt
head -20 output.txt






