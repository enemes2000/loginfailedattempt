#!/bin/bash

  
echo 'creating topic'
kafka-topics --topic login \
 --replication-factor 1 \
 --create \
 --bootstrap-server localhost:9092 \
 --partitions 3 \
 
