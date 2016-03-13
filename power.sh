#!/bin/sh

make clean
make powerdays
hadoop jar powerdays.jar
make get
make show

