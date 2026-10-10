
import csv
import matplotlib.pyplot as plt
from collections import defaultdict

data = list(csv.DictReader(open("experiments/results/scaling.csv")))

groups = defaultdict(list)
for row in data:
    groups[(row["query_label"], int(row["peer_count"]))].append(
        float(row["elapsed_seconds"])
    )

for query in sorted({row["query_label"] for row in data}):
    peers = sorted(p for q, p in groups if q == query)
    averages = [
        sum(groups[(query, p)]) / len(groups[(query, p)])
        for p in peers
    ]
    plt.plot(peers, averages, marker="o", label=query)

plt.xlabel("Peer count")
plt.ylabel("Average elapsed time (seconds)")
plt.title("IDSS query latency by peer count")
plt.legend()
plt.grid(True)
plt.savefig("experiments/results/latency-by-peers.png", dpi=150)
