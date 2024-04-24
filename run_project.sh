#!/bin/bash

# Start Zookeeper in a new terminal
echo "Starting Zookeeper..."
gnome-terminal -- /home/hdoop/kafka/bin/zookeeper-server-start.sh /home/hdoop/kafka/config/zookeeper.properties

# Start Kafka Server in a new terminal
echo "Starting Kafka Server..."
sleep 10  # Give Zookeeper some time to start
gnome-terminal -- /home/hdoop/kafka/bin/kafka-server-start.sh /home/hdoop/kafka/config/server.properties

# Wait for Kafka Server to start
sleep 10

# Start MongoDB
start_terminal "mongod --dbpath /var/lib/mongodb"

# Run producer in a new terminal
echo "Running producer..."
gnome-terminal -- python3 /home/hdoop/kafka/Assignment2/producer.py

# Run consumer 1 in a new terminal
echo "Running consumer 1..."
gnome-terminal -- python3 /home/hdoop/kafka/Assignment2/aprioriAlgo.py

# Run consumer 2 in a new terminal
echo "Running consumer 2..."
gnome-terminal -- python3 /home/hdoop/kafka/Assignment2/pcyAlgo.py

# Run consumer 3 in a new terminal
echo "Running consumer 3..."
gnome-terminal -- python3 /home/hdoop/kafka/Assignment2/spadeAlgo.py

echo "All processes started."




