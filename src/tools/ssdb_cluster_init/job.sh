#!/bin/bash

while(true)
do
    echo "ok"
    python ./ssdb_cluster_init.py  -c /home/wangchangqing/ -z "192.168.217.11:2181"
    sleep 5
done
