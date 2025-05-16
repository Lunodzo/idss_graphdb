# InnoCyPES Distributed Data Storage Service (IDSS)

IDSS is a distributed data storage and query execution service that allows clients 
to submit queries to an overlay network of peers. The queries are executed in a 
decentralised manner, with results merged and returned to the client. The system uses 
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
| **libp2p** | Peer-to-peer communication between nodes and their streams for message exchange |
| **Kademlia DHT** | Distributed hash table for peer discovery and data routing. In this case, it is also used to broadcast queries and merge the results |
| **EliasDB** | Graph-based database for database systems. Embedded on top of P2P overlay |
| **Protocol Buffers (Protobuf)** | Data serialization for efficient query and result transfer |
| **Streams (libp2p)** | Message exchange between peers |
| **Bash Script** | Automating peer startup, data loading, and querying |


#Exploring Danish Power Systems
In this version of IDSS, we are exploring its implementation based on one of the use cases at Green Power Denmark, herein referred to as GPD. GDP is Denmark's green business organisation and acts as a voice for the Danish energy sector. Together with its members, they work to ensure that Denmark is electrified with green electricity as soon as possible. It has approximately 1,500 members and represents the energy industry, large and small owners and installers of energy technology, as well as the companies that operate the Danish electricity grid and trade in energy. Founded in 2022, through a merger of Dansk Energi, Wind Denmark and Dansk Solkraft. With this, GPD has become a custodian of the majority of datasets that are relevant to our use case. The current data flow goes between GPD and DSOs, while the  other data is collected from other external stakeholders.

#Use case description
In the context of Asset Management, the reliability of Medium_Voltage_Cables (MVC) is crucial to the management of the distribution systems. MV refers thereby to the voltage range of 1 − 25kv, which contributes strongly to state-of-the-art reliability indicators, such as the System Average Interruption Duration Index (SAIDI) and the System Average Interruption Frequency Index (SAIFI). In addition, studies indicate an increase in age-related failure rates for paper-insulated lead-covered (PILC) cables. Furthermore, the increasing electrification of the mobility and heating sectors, in combination with increasing decentralised production, requires a modernisation of the MV cable network in general. Consequently, tools are required that prioritise investments properly. Data plays an important role in the success of these requirements. Although plenty of data already exists, how they are managed still limits the realisation of its full potential. A recommended data description is presented below, which could be adopted into several data models. In particular, this use case will be based on relational and graph databases. More description about the relevance of this use case is presented in our previous work [here](https://ieeexplore.ieee.org/abstract/document/10863105).

#Agnostic Data Model for Asset Management (MVC)
In the context of reliability prediction of MVCs, there is an opportunity for DSOs to standardise data collection. This repository provides a data model created to facilitate this standardisation. The data model ensures that all data adheres to specified formats and relationships, fulfilling a prerequisite for decentralised data storage.

A simplified visualisation of the data schema is provided below:
![image](https://github.com/H2020-InnoCyPES-ITN/Decentralised-Relational-Data-Model-for-Reliability-Studies-of-Medium-Voltage-Cables/assets/101191232/c25c021a-8931-4b33-aaed-9b638c2c58ba)

The diagram is colour-coded to represent different components:

- Yellow: Topics grid components
- Blue: Cable events
- Red: External cable event impacts
- Green: Cable placement conditions

#Key Entities/objects and Attributes/properties


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
Peer node can be launched in two ways: first, by running each node separately. When this is done, each node should run on its terminal. Second, by using the `start_peers.sh` script.

## Launch peer nodes separately
```sh
cd server
go run .
```
or 
```sh
go run idss_main.go
```
## Launch peer nodes using a bash script
Within the `server` directory, run the bash script by specifying the number of peers you want to run
```sh
./start_peers.sh <number_of_peers>
```
Each node will generate its folder directory for database in `idss_graph_db`, and will generate its own log file in the `log` directory.

Sample graph nodes and edges are generated using `generate_data.py`, and the data model is mapped by the generated `.json` file during peer launching.

Note: At least two peers should be started for an overlay to work since a connection among peers must be established.

# Running queries
All queries are submitted by the IDSS client. A client will connect to any peer that is 
running in an overlay. To be able to connect, a client needs to know only the address of 
one of the peers that can be found in the log file.

From the IDSS root directory, change the directory to the client to proceed.

```sh
cd client
go run . -s <server_address>
```
A client should be able to connect. Proceed with query submission. Samples are given below. A number after the comma represents a TTL value (A time that you are willing to wait.
```sh
get Client, 3
get Consumption, 7
```
More advanced queries following EQL syntax are included in the `info.txt` file. A limited support of 
Aggregate functions like `min`, `max`, `sum` and `avg` will also be finalised soon.

## Query results
Query results are stored in the `results` directory. A directory that is generated once a query is submitted 
to an overlay. An `.xml` file named after your client ID, with the time at which a query was submitted, contains the 
results set following the submitted query.
