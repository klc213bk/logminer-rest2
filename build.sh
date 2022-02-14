#!/bin/bash

set -e

echo "start to build kafka-connect-logminer"

cd ../kafka-connect-logminer
mvn clean package

cp ./target/kafka-connect-logminer-1.0.jar ../logminer-rest2/connectors

cd ../logminer-rest2

echo "start to build logminer-rest"
mvn clean package


