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
for i in range(1, 11):
    clients.append({
        "kind": "Client",
        "key": i,
        "name": fake.name(),
        "contract_number": fake.unique.random_int(min=100, max=999),
        "power": random.randint(100, 200)
    })

# Generate consumption records data
consumptions = []
for i in range(1, 101):
    consumptions.append({
        "kind": "Consumption",
        "key": i,
        "timestamp": fake.unix_time(),
        "measurement": random.randint(200, 1000)
    })

# Generate relationships (edges) between clients and consumption records
edges = []
for client in clients:
    consumption_keys = random.sample(range(1, 101), 10)  # Each client has 10 consumption records
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

# Combine all data into one dictionary
data = {
    "nodes": clients + consumptions,
    "edges": edges
}

# Output the generated data as JSON
with open(output_file, "w") as f:
    json.dump(data, f, indent=4)

print(f"Data saved to {output_file}")
