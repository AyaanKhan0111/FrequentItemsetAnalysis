from kafka import KafkaConsumer
import itertools
import json
from pymongo import MongoClient

# Function to generate candidate itemsets of size k
def generate_candidate_itemsets(itemsets, k):
    candidates = []
    # Generate combinations of size k
    for subset in itertools.combinations(itemsets, k):
        candidates.append(subset)
    return candidates

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'fdm'

# Define minimum support threshold
min_support = 2

# Initialize transactions window
transactions_window = []
max_transactions = 1000  

# MongoDB connection settings
mongo_client = MongoClient('localhost', 27017)  # MongoDB default connection
db = mongo_client['my_database']  #database name
collection = db['apriori_results']  #collection name

# Kafka consumer configuration
consumer = KafkaConsumer(topic,
                         group_id='apriori_consumer',
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

    # Output frequent itemsets of size 2 (pairs)
    frequent_itemsets = []
    for k in range(2, 3):  # Output pairs
        # Generate candidate itemsets of size k
        candidate_itemsets = generate_candidate_itemsets(item_counts.keys(), k)

        # Output frequent itemsets of size k
        for itemset in candidate_itemsets:
            # Check if all elements in the itemset have minimum support
            if all(item_counts.get(item, 0) >= min_support for item in itemset):
                frequent_itemsets.append(itemset)
                print(f"Frequent itemset of size {k}: {itemset}")

    # Store frequent itemsets in MongoDB
    for itemset in frequent_itemsets:
        collection.insert_one({'itemset': itemset})

# Close Kafka consumer
consumer.close()