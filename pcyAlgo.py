from kafka import KafkaConsumer
import itertools
import json
import mmh3  # Import MurmurHash3 library for hashing
from pymongo import MongoClient  

# Function to generate candidate itemsets of size k
def generate_candidate_itemsets(itemsets, k):
    candidates = []
    # Generate combinations of size k
    for subset in itertools.combinations(itemsets, k):
        candidates.append(subset)
    return candidates

# Function to generate hash table from item pairs
def generate_hash_table(item_pairs):
    hash_table = {}
    for pair in item_pairs:
        hash_value = mmh3.hash(str(pair), signed=False) % hash_table_size
        hash_table[hash_value] = hash_table.get(hash_value, 0) + 1
    return hash_table

# Function to filter frequent itemsets using bitmap
def filter_frequent_itemsets(item_counts):
    frequent_items = set()
    for item, count in item_counts.items():
        if count >= min_support:
            frequent_items.add(item)
    return frequent_items

# MongoDB connection settings
mongo_client = MongoClient('mongodb://localhost:27017/')  # Connect to MongoDB client
db = mongo_client['my_database']  # database
collection = db['pcy_results']  # collection

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'fdm'

# Define minimum support threshold
min_support = 2

# Initialize transactions window
transactions_window = []
max_transactions = 1000  

# Initialize PCY parameters
hash_table_size = 1000  
bitmap = [0] * hash_table_size

# Kafka consumer configuration
consumer = KafkaConsumer(topic,
                         group_id='pcy_consumer',
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

    # Count occurrences of each item
    item_counts = {}
    for transaction in transactions_window:
        # Increment count for each item in the transaction
        for item in transaction:
            item_counts[item] = item_counts.get(item, 0) + 1

    # Generate hash table from item pairs
    item_pairs = itertools.combinations(item_counts.keys(), 2)
    hash_table = generate_hash_table(item_pairs)

    # Filter frequent items using bitmap
    frequent_items = filter_frequent_itemsets(item_counts)

    # Output frequent itemsets of size 2 (pairs)
    frequent_pairs = []
    for pair in itertools.combinations(frequent_items, 2):
        hash_value = mmh3.hash(str(pair), signed=False) % hash_table_size
        if hash_table.get(hash_value, 0) >= min_support:
            frequent_pairs.append(pair)
            print(f"Frequent pair: {pair}")

    # Store frequent pairs in MongoDB if not empty
    if frequent_pairs:
        collection.insert_many([{"pair": pair} for pair in frequent_pairs])
        

# Close Kafka consumer
consumer.close()