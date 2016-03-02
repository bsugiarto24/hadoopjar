#!/bin/sh

make clean
make activity
hadoop jar activity.jar -libjars org.json-20120521.jar,json-mapreduce-1.0.jar out.txt test/output/
make get
make show

