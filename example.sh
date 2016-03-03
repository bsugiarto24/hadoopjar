#!/bin/sh

make clean
make example
hadoop jar example.jar -libjars org.json-20120521.jar,json-mapreduce-1.0.jar test.json test/output/
make get
make show

