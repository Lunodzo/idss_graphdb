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
import argparse
from faker import Faker

# Initialize Faker for generating fake data
fake = Faker()

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Generate fake data for IDSS graph database")
parser.add_argument("output_file", help="Path to the output JSON file")
parser.add_argument("--num-clients", type=int, default=4, help="Number of Client nodes")
parser.add_argument("--num-consumptions", type=int, default=9, help="Number of Consumption nodes")
parser.add_argument("--edges-per-client", type=int, default=2, help="Number of edges per Client")
args = parser.parse_args()

output_file = args.output_file
num_clients = args.num_clients
num_consumptions = args.num_consumptions
edges_per_client = min(args.edges_per_client, num_consumptions)  # Ensure valid edge count

# Validate inputs
if num_clients < 0 or num_consumptions < 0 or edges_per_client < 0:
    print("Error: Number of clients, consumptions, or edges cannot be negative")
    sys.exit(1)
if edges_per_client > num_consumptions:
    print(f"Warning: edges-per-client ({edges_per_client}) exceeds num-consumptions ({num_consumptions}). Using {num_consumptions} instead.")
    edges_per_client = num_consumptions

# Generate clients data
clients = []
client_keys = set()  # Track unique keys
for i in range(1, num_clients + 1):
    key = i
    while key in client_keys:  # Ensure unique key
        key = random.randint(1, 1000000)
    client_keys.add(key)
    clients.append({
        "kind": "Client",
        "key": key,
        "client_name": fake.name(),
        "contract_number": fake.unique.random_int(min=100, max=9999),
        "power": random.randint(0, 2000)
    })

# Generate consumption records data
consumptions = []
consumption_keys = set()
for i in range(1, num_consumptions + 1):
    key = i
    while key in consumption_keys:
        key = random.randint(1, 1000000)
    consumption_keys.add(key)
    consumptions.append({
        "kind": "Consumption",
        "key": key,
        "timestamp": fake.unix_time(),
        "measurement": random.randint(0, 10000000)
    })

print(f"Generated {len(clients)} Client nodes and {len(consumptions)} Consumption nodes")

# Generate relationships (edges) between clients and consumption records
edges = []
for client in clients:
    # Select random consumption keys, ensuring they exist
    selected_keys = random.sample(list(consumption_keys), min(edges_per_client, len(consumption_keys)))
    for consumption_key in selected_keys:
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

print(f"Generated {len(edges)} edges")

# Stream JSON output to file
def stream_json_to_file(filename, clients, consumptions, edges):
    with open(filename, "w") as f:
        f.write("{")  # Start JSON object
        # Write nodes
        f.write('"nodes": [')
        nodes = clients + consumptions
        for i, node in enumerate(nodes):
            json.dump(node, f)
            if i < len(nodes) - 1:
                f.write(",")
        f.write("],")
        # Write edges
        f.write('"edges": [')
        for i, edge in enumerate(edges):
            json.dump(edge, f)
            if i < len(edges) - 1:
                f.write(",")
        f.write("]}")

try:
    stream_json_to_file(output_file, clients, consumptions, edges)
    print(f"Data saved to {output_file}")
except Exception as e:
    print(f"Error writing JSON file: {e}")
    sys.exit(1)