#!/bin/bash
for i in `seq 1 20`;
do
	python process.py $i.csv
done
