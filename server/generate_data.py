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
for i in range(1, 11): # Adjust the number of clients here
    clients.append({
        "kind": "Client",
        "key": i,
        "name": fake.name(),
        "contract_number": fake.unique.random_int(min=100, max=9999999),
        "power": random.randint(100, 200000)
    })

# Generate consumption records data
consumptions = []
for i in range(1, 11): # Adjust the number of consumption records here
    consumptions.append({
        "kind": "Consumption",
        "key": i,
        "timestamp": fake.unix_time(),
        "measurement": random.randint(200, 1000000)
    })
    
print("Stored a total of %d consumption records", len(consumptions))

# Generate relationships (edges) between clients and consumption records
# random.sample(range(1, x), y)
# Each client has y consumption records. Change y to adjust the number of consumption records per client. 
# range(1, x) is the range of consumption records keys. x should be less than the total number of consumption records.
edges = []
for client in clients:
    consumption_keys = random.sample(range(1, 51), 0)  
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
