#!/bin/bash

KAFKA_HOME=/opt/kafka_2.13-3.1.0

export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:./config/health/connect-log4j.properties"

${KAFKA_HOME}/bin/connect-standalone.sh ./config/health/standalone_connect.properties ./config/health/OracleSourceConnector.properties
