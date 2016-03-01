#!/bin/sh

make clean
make summaries
hadoop jar summaries.jar -libjars org.json-20120521.jar,json-mapreduce-1.0.jar example.txt test/output/
make get
make show

