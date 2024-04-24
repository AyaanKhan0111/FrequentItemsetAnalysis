from kafka import KafkaConsumer
from collections import defaultdict
import json
from pymongo import MongoClient  # Import MongoClient for MongoDB integration

# Function to generate all possible subsequences of a sequence
def generate_subsequences(sequence):
    subsequences = []
    for i in range(len(sequence)):
        for j in range(i + 1, len(sequence) + 1):
            subsequences.append(sequence[i:j])
    return subsequences

# Function to generate candidate sequences of length k+1 from frequent sequences of length k
def generate_candidate_sequences(frequent_sequences, k):
    candidates = []
    for seq1 in frequent_sequences:
        for seq2 in frequent_sequences:
            if seq1[:-1] == seq2[:-1] and seq1[-1] != seq2[-1]:
                candidate = seq1 + [seq2[-1]]
                if all(subseq in frequent_sequences for subseq in generate_subsequences(candidate)):
                    candidates.append(candidate)
    return candidates

# MongoDB connection settings
mongo_client = MongoClient('mongodb://localhost:27017/')  # Connect to MongoDB client
db = mongo_client['my_database']  # Specify the database
collection = db['spade_results']  # Specify the collection

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'fdm'

# Define minimum support threshold
min_support = 2

# Initialize transactions window
transactions_window = []
max_transactions = 1000  # Adjust the maximum number of transactions as needed

# Kafka consumer configuration
consumer = KafkaConsumer(topic,
                         group_id='spade_consumer',
                         bootstrap_servers=[bootstrap_servers],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Main loop to consume messages from Kafka
for message in consumer:
    # Get the transaction data from the Kafka message
    transaction = message.value["also_buy"]

    # Update transactions window
    transactions_window.append(transaction)

    # Check if the maximum number of transactions is reached
    if len(transactions_window) >= max_transactions:
        break

    # Generate frequent sequences
    sequence_counts = defaultdict(int)
    for transaction in transactions_window:
        subsequences = generate_subsequences(transaction)
        for subsequence in subsequences:
            sequence_counts[tuple(subsequence)] += 1

    frequent_sequences = [list(seq) for seq, count in sequence_counts.items() if count >= min_support]

    # Output frequent sequences
    print("Frequent Sequences:")
    for seq in frequent_sequences:
        print(seq)

    # Generate candidate sequences of length k+1 from frequent sequences of length k
    k = 1
    while True:
        candidate_sequences = generate_candidate_sequences(frequent_sequences, k)
        if not candidate_sequences:
            break
        frequent_sequences = []
        for seq in candidate_sequences:
            count = sum(1 for transaction in transactions_window if all(subseq in transaction for subseq in generate_subsequences(seq)))
            if count >= min_support:
                frequent_sequences.append(seq)
                print(seq)
        k += 1

    # Store frequent sequences in MongoDB
    if frequent_sequences:
        collection.insert_many([{"sequence": seq} for seq in frequent_sequences])

# Close Kafka consumer
consumer.close()