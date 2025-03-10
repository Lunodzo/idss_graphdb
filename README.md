# InnoCyPES Distributed Data Storage Service (IDSS)

IDSS is a distributed data storage and query execution service that allows clients 
to submit queries to an overlay network of peers. The queries are executed in a 
decentralized manner, with results merged and returned to the client. The system uses 
a best-effort approach with a time-to-live (TTL) mechanism to ensure efficient query 
execution.

## Features
- Peer-to-peer overlay using libp2p and Kademlia DHT
- Golang-based implementation with libp2p networking
- Graph database using eliasdb
- Distributed query execution (using EQL) across multiple peers in a decentralised manner
- Query merging and result aggregation
- Time-to-live (TTL) mechanism for query execution control

## Technologies Used
| Technology | Purpose |
|------------|---------|
| **Golang** | Core implementation of the system |
| **libp2p** | Peer-to-peer communication between nodes and its streams for message exchange |
| **Kademlia DHT** | Distributed hash table for peer discovery and data routing. In this case it is also used to broacast queries and merging the results |
| **EliasDB** | Graph-based database for database systems. Embedded ontop of P2P overlay |
| **Protocol Buffers (Protobuf)** | Data serialization for efficient query and result transfer |
| **Streams (libp2p)** | Message exchange between peers |
| **Bash Script** | Automatomating peer startup, data loading, and querying |

# Installation and Setup
## Dependencies
Before running IDSS, ensure you have the following installed:
- Golang (1.23.1) [Check how to install](https://go.dev/doc/install)
- Python3 (It should be installed within your Linux Distribution, it should be okay to simply update/upgrade)
- Git
## Clone the repository 
```sh
git clone https://github.com/Lunodzo/idss_graphdb.git
```
## Install dependencies
```sh
go mod tidy
```
# Running the Peer Node(s)
Peer node can be launched in two ways, first, by running each node separately. When this is done, each node should run on its own terminal. Second, by using the `start_peers.sh` script.

## Launch peer nodes separately
```sh
cd server
go run .
```
or 
```sh
go run idss_main.go
```
## Launch peer nodes using bash script
Within the `server` directory, run bash script by specifying the number of peers you want to run
```sh
./start_peers.sh <number_of_peers>
```
Each node will generate its own folder directory for database in `idss_graph_db`, and will generate its own log file in the `log` directory.

Sample graph nodes and edges is generated using `generate_data.py` and the data model is mapped by the generated `.json` file during peer launching.

Note: Atleast two peers should be started for an overlay to work since a connection among peers must be established.

# Running queries
All queries are submitted by the IDSS client. A client will connect to any peer that is 
running in an overlay. To be able to connect a client need to know only the address of 
one of peers that can be found in the log file.

From the IDSS root directory, change directory to client to proceed.

```sh
cd client
go run . -s <server_address>
```
A client should be able to connect. Proceed with query submission. Samples are given below. A number after comma represents a TTL value (A time that you are willing to wait.
```sh
get Client, 3
get Consumption, 7
```
More advanced queries following EQL syntax are included in the `info.txt` file. A limited support of 
aggregate functions like `min`, `max`, `sum` and `avg` is also possible using this version.

## Query results
Query results are stored in the `results` directory. A directory that is generated once a query is submitted 
to an overlay. A `.xml` file named after your client ID with the time to which a query was submitted contains 
results set following the submitted query.

@2025, University of Salento
Authors:
1. Lunodzo Justine Mwinuka
2. Massimo Cafaro
3. Italo Epicoco
