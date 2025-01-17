#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' 

# Update the below variables with your environment details
ZOOKEEPER_HOST="localhost"
ZOOKEEPER_PORT=2181
KAFKA_HOST="localhost"
KAFKA_PORT=9092
FLINK_SERVER="localhost:8081"
METABASE_SERVER="localhost:3000"

check_scala() {
  if command -v scala &> /dev/null; then
    echo -e "${GREEN}Scala is installed. Version:${NC}"
    scala -version
  else
    echo -e "${RED}Scala is not installed.${NC}"
  fi
}

check_java() {
  if command -v java &> /dev/null; then
    echo -e "${GREEN}Java is installed. Version:${NC}" 
    java -version
  else
    echo -e "${RED}Java is not installed.${NC}"
  fi
}

check_maven() {
    if command -v mvn &> /dev/null; then
        echo -e "${GREEN}Maven is installed.${NC}"
        echo -e "${GREEN}Maven version:${NC}"
        mvn -v
    else
        echo -e "${RED}Maven is NOT installed.${NC}"
    fi
}

check_curl() {
    if command -v curl &> /dev/null; then
        echo -e "${GREEN}Curl is installed.${NC}"
        echo -e "${GREEN}Curl version:${NC}"
        curl --version | head -n 1
    else
        echo -e "${RED}Curl is NOT installed.${NC}"
    fi
}

check_zookeeper() {
  if nc -z $ZOOKEEPER_HOST $ZOOKEEPER_PORT; then
    echo -e "${GREEN}Zookeeper is running on port $ZOOKEEPER_PORT.${NC}"
  else
    echo -e "${RED}Zookeeper is not running.${NC}"
  fi
}


check_kafka() {
  if nc -z $KAFKA_HOST $KAFKA_PORT; then
    echo -e "${GREEN}Kafka is running on port $KAFKA_PORT.${NC}"
  else
    echo -e "${RED}Kafka is not running.${NC}"
  fi
}

check_flink_process() {
  response=$(curl -s -o /dev/null -w "%{http_code}" http://$FLINK_SERVER/overview)
  if [ "$response" -eq 200 ]; then
      echo -e "${GREEN}Flink is running on $FLINK_SERVER${NC}"
  else
      echo -e "${RED}Flink is NOT running or unreachable on $FLINK_SERVER${NC}"
  fi
}

check_metabase_process() {
  response=$(curl -s -o /dev/null -w "%{http_code}" http://$METABASE_SERVER/overview)
  if [ "$response" -eq 200 ]; then
      echo -e "${GREEN}Metabase is running on $METABASE_SERVER${NC}"
  else
      echo -e "${RED}Metabase is NOT running or unreachable on $METABASE_SERVER${NC}"
  fi
}

echo -e "${BLUE}>>>>>>>> Prerequisites check started >>>>>>>>${NC}"
check_scala
check_java
check_maven
check_curl
check_flink_process
check_zookeeper
check_kafka
check_metabase_process
echo -e "${BLUE}>>>>>>>> Prerequisites check completed >>>>>>>>${NC}"
