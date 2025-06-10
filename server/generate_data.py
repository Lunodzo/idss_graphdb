# A simple script to generate random data for the graph database. 
# Modify the number of clients, consumption records, and consumption 
# records per client as needed. The script uses the Faker library to 
# generate fake data. The generated data is saved as a JSON file. 
# The script takes one argument, the output file name.

# For simple testing, user 10 clients and 100 consumption records

# Usage: python3 generate_data.py <output_file>

# Copyright 2023-2027, University of Salento, Italy.
# All rights reserved.

import random
import json
import sys
from faker import Faker

fake = Faker()

if len(sys.argv) < 2:
    print("Usage: python3 generate_data.py <output_file>")
    sys.exit(1)

output_file = sys.argv[1]

# Generate clients data
clients = []
for i in range(1, 1001): # Adjust the number of clients here
    clients.append({
        "kind": "Client",
        "key": i,
        "client_name": fake.name(),
        "contract_number": fake.unique.random_int(min=100, max=9999),
        "power": random.randint(0, 2000)
    })

# Generate consumption records data
consumptions = []
for i in range(1, 10001): # Adjust the number of consumption records here
    consumptions.append({
        "kind": "Consumption",
        "key": i,
        "timestamp": fake.unix_time(),
        "measurement": random.randint(0, 10000000)
    })
    
print("Stored a total of %d consumption records", len(consumptions))

# Generate relationships (edges) between clients and consumption records
# random.sample(range(1, x), y)
# Each client has y consumption records. Change y to adjust the number of consumption records per client. 
# range(1, x) is the range of consumption records keys. x should be less than the total number of consumption records.
edges = []
for client in clients:
    consumption_keys = random.sample(range(1, 10000), 10)  
    for consumption_key in consumption_keys:
        edges.append({
            "key": f"e{len(edges) + 1}",
            "kind": "belongs_to",
            "end1key": client["key"],
            "end1kind": "Client",
            "end1role": "owner",
            "end1cascading": random.choice([True, False]),
            "end2key": consumption_key,
            "end2kind": "Consumption",
            "end2role": "usage",
            "end2cascading": True
        })
        
print("Stored a total of %d edges", len(edges))

# Combine all data into one dictionary
data = {
    "nodes": clients + consumptions,
    "edges": edges
}

# Output the generated data as JSON
with open(output_file, "w") as f:
    json.dump(data, f, indent=4)

print(f"Data saved to {output_file}")
