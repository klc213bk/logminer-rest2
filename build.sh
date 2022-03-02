#!/bin/bash

set -e

echo "start to build kafk-connect-logminer2"

cd ../kafka-connect-logminer2
mvn clean package

cd ../logminer-rest2

cp ../kafka-connect-logminer2/target/kafka-connect-logminer2-2.0.jar ./connectors
cp ../kafka-connect-logminer2/target/kafka-connect-logminer2-2.0.jar /data/v2/kafka/connectors


echo "start to build logminer-rest2"
mvn clean package


