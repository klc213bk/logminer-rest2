#!/bin/bash

export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:./config/connect-log4j.properties"

java -jar -Dspring.profiles.active=prod target/logminer-rest2-1.0.jar --spring.config.location=file:config/ & 
