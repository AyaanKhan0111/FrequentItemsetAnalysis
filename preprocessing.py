import json

with open('Sampled_Amazon_eta3.json', 'r') as inputFile, open('preprocessed_data.json', 'w') as output:
    for itemsets in inputFile:
        data = json.loads(itemsets)
        
        preprocessed_data = {
            "also_buy": data.get("also_buy", []),
            "asin": data.get("asin", "")
        }
        
        json.dump(preprocessed_data, output)
        output.write('\n')  