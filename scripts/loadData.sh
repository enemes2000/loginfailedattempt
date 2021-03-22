#!/bin/bash
 
echo 'loading data'
kafka-avro-console-producer --topic login --bootstrap-server broker:29092 --property value.schema="$(< /tmp/data/login.avsc)" < /tmp/data/login.json
