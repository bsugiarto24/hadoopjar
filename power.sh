#!/bin/sh

make clean
make rmtemp
make powerdays
hadoop jar powerdays.jar
make get
make show

