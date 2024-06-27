#!/bin/bash
. ~/.bash_profile
KAFKA_HOME=/app/CMS/INT_BPM/kafka_2.11-2.4.1
KAFKA_BROKER=192.168.6.47:9092
CONFIG_FILE="../config/$1"  # Config file name passed as argument
BATCH_SIZE=5  # Number of topics per batch

function delete_topic() {
    local TOPIC=$1
    echo "Deleting Topic: $TOPIC"
    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic $TOPIC
    if [ $? -ne 0 ]; then
        echo "Kafka Topic Delete Unsuccessful for $TOPIC"
        exit 1
    fi
}

function recreate_topic() {
    local TOPIC=$1
    local PARTITION=$2
    echo "Recreating Topic: $TOPIC"
    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create --topic $TOPIC --partitions $PARTITION --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000
    if [ $? -ne 0 ]; then
        echo "Kafka Topic Recreation Unsuccessful for $TOPIC"
        exit 1
    fi
}

function process_batch() {
    local BATCH=("$@")
    for TOPIC_INFO in "${BATCH[@]}"; do
        TOPIC=$(echo $TOPIC_INFO | cut -d ':' -f1)
        PARTITION=$(echo $TOPIC_INFO | cut -d ':' -f2)
        delete_topic $TOPIC
    done
    sleep 60
    for TOPIC_INFO in "${BATCH[@]}"; do
        TOPIC=$(echo $TOPIC_INFO | cut -d ':' -f1)
        PARTITION=$(echo $TOPIC_INFO | cut -d ':' -f2)
        recreate_topic $TOPIC $PARTITION
    done
}

function batch_process() {
    local FILE=$1
    mapfile -t TOPICS < $FILE
    local BATCH=()
    for (( i=0; i<${#TOPICS[@]}; i++ )); do
        BATCH+=("${TOPICS[$i]}")
        if (( (i + 1) % BATCH_SIZE == 0 )); then
            process_batch "${BATCH[@]}" &
            BATCH=()
        fi
    done
    if (( ${#BATCH[@]} > 0 )); then
        process_batch "${BATCH[@]}" &
    fi
    wait
}

echo "Message cleanup for $1 Starting"

batch_process $CONFIG_FILE

echo "Message cleanup for $1 Completed"
