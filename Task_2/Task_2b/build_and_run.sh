#!/bin/bash -e

# Compile and package the source code
mvn -q package
# Run with Spark
spark-submit target/task2b-1.0-SNAPSHOT-uber.jar
# Remove build outputs
mvn -q clean
