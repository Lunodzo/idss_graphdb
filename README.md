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


# Exploring Danish Power Systems
In this version of IDSS, we are exploring its implementation based on one of the use cases at Green Power Denmark, herein referred to as GPD. GDP is Denmark's green business organisation and acts as a voice for the Danish energy sector. Together with its members, they work to ensure that Denmark is electrified with green electricity as soon as possible. It has approximately 1,500 members and represents the energy industry, large and small owners and installers of energy technology, as well as the companies that operate the Danish electricity grid and trade in energy. Founded in 2022, through a merger of Dansk Energi, Wind Denmark and Dansk Solkraft. With this, GPD has become a custodian of the majority of datasets that are relevant to our use case. The current data flow goes between GPD and DSOs, while the  other data is collected from other external stakeholders.

# Use case description
In the context of Asset Management, the reliability of Medium_Voltage_Cables (MVC) is crucial to the management of the distribution systems. MV refers thereby to the voltage range of 1 − 25kv, which contributes strongly to state-of-the-art reliability indicators, such as the System Average Interruption Duration Index (SAIDI) and the System Average Interruption Frequency Index (SAIFI). In addition, studies indicate an increase in age-related failure rates for paper-insulated lead-covered (PILC) cables. Furthermore, the increasing electrification of the mobility and heating sectors, in combination with increasing decentralised production, requires a modernisation of the MV cable network in general. Consequently, tools are required that prioritise investments properly. Data plays an important role in the success of these requirements. Although plenty of data already exists, how they are managed still limits the realisation of its full potential. A recommended data description is presented below, which could be adopted into several data models. In particular, this use case will be based on relational and graph databases. Hence, generic terms will be used interchangeably with those of relational and graph databases. Specifically, the terms entity, node and object will be used interchangeably. This is also the case with the words attributes, properties and characteristics. More description about the use case and its relevance is presented in our previous work [here](https://ieeexplore.ieee.org/abstract/document/10863105).

## Agnostic Data Model for Asset Management (MVC)
In the context of reliability prediction of MVCs, there is an opportunity for DSOs to standardise data collection. This repository provides a data model created to facilitate this standardisation. The data model ensures that all data adheres to specified formats and relationships, fulfilling a prerequisite for decentralised data storage.

A simplified visualisation of the data schema is provided below:

![image](https://github.com/H2020-InnoCyPES-ITN/Decentralised-Relational-Data-Model-for-Reliability-Studies-of-Medium-Voltage-Cables/assets/101191232/c25c021a-8931-4b33-aaed-9b638c2c58ba)

The diagram is colour-coded to represent different components:

- Yellow: Topics grid components
- Blue: Cable events
- Red: External cable event impacts
- Green: Cable placement conditions

# Key Entities/objects and Attributes/properties
## DSOs
The DSO entity is essential, it maps the DSO object into an object, in this way, different network characteristics and management decisions can be investigated. Further, it isn’t sufficient to model DSOs as a property of other objects, because this would limit the possibility to store DSO-specific properties, such as name, DSO supply area or reliability indices and others.

## Substations
To model substation connections within the MV cable network, a substation entity is created with attributes such as DSO-id, voltage levels, installation date, station type, and coordinates. As shown in the schema, the substation is a super-class from which the subclasses ”primary station” and ”secondary substation” are derived. In this way, all of the attributes of the substation entity are inherited, while specific attributes per sub-entity types can be stored as well, such as the ”primary parent station” for the ”secondary substation”.

## MVC Systems
The ”MVC systems” consist of the properties: station-from-id and station-to-id, which can thereby link to both main or/and secondary substation and link to high-level attributes such as operating DSO. Beyond that, to align operational parameters and to ensure naming conventions, further attributes are introduced, such as operating voltage, average and max loading.

## MVC subsections
The entity ”MVC subsections” links to the ”MV cable systems” (through its identifying key) entity and thus reflects that the system is divided into one or more subsections. Moreover, a list of subsection-specific parameters is added, such as conductor material and type, number of conductors, insulation, manufacturer, in-service date, length, and coordinates. Tracking the connection to other subsections is also possible. Lastly, two boolean attributes are introduced that reflect if the MV section was installed due to a repair and if the section is still in operation.

## MVC Joints
MV subsections are connected via cable joints. These can represent a "weak entity" in the cable system and present additional attributes such as joint type and location. They should thus be integrated into the data model as a separate entity.

## Cable Failures and Repairs
To model events that impact one specific MV cable subsection, such as failures and repairs, a cable event entity is created. Via a foreign key (reflection to relational DB), it links to the affected MV cable subsection and obtains date attributes, such as event start and event end. Next, the Sub-entities ”Cable Failures” and ”Cable repairs” can be created from the ”Cable Event” entity. The failures contain attributes such as failure type, cause and location, whereas the repairs link to repair specifications and the related failure event.

## External Events
Despite events that always involve the cable, the data schema also includes external events, such as digging activities and weather-based events, including lightning, heatwaves, cold waves, floods, etc., that might have an impact on the cable reliability and thus need to be reflected in the database schema. Consequently, it is proposed that the creation of an entity called ”external events” from which all subsequent external event types inherit their start and end times. Due to different ways of storing geographical information, e.g. polygons for digging activities and point coordinates for lightning, the location attributes are saved on the sub-entity level. Furthermore, for each external event type, more specific attributes are added, such as utility and digging type for the digging activities or number of strokes and intensity for lightning.

## Cable external event impacts
One key requirement for predicting the reliability of MV cables is to query external events that have affected them. To reflect this, an entity ”Cable External Event Impacts” is created that links MV subsections to external events. The definition of a cable being affected by an event is then defined, given the location and time.

## Location-Based Drivers
Other static entities, such as roads, rails, water bodies, ground water level, etc., might also influence the reliable operation of the MV cables if they are in proximity. Because they are location-specific, they can be summarised in the entity of ”location-based drivers”. More attributes can be added on a sub-entity level, such as road, rail, or waterbody type. This study proposes to add environmental observation to the entity of location-based drivers. In this way, favourable or unfavourable conditions with regard to environmental parameters such as temperature, humidity or precipitation can also be tracked and taken into account.

## Cable Placement Conditions
Within the ”Cable Placement Conditions” entity, the previously described location based drivers are spatially linked to the MV cable subsections. Consequently, this facilitates data retrieval of the location-specific placement conditions of the cables.

## Derived Attributes and Properties
Many data requests related to reliability prediction of MV cables require aggregated information on either MV cable subsection, MV cable system, or at the cable failure level. Therefore the computation of derived attributes, such as number of subsections or joints per MV cable system can be beneficial for several reasons. First, the computation of derived attributes can be more efficient than doing the same computation externally. Further it can be expected that if the derived attributes are used in multiple queries or reports, computing them in SQL (Structured Query Language) can improve code re-usability and maintainability. Moreover, the derived attributes can help to maintain Data integrity, as a computation within the DBMS (Data Base Management System), can ensure that the computations are based directly on the source data. One use case for derived attributes is given by the aggregation of information on the MV cable system level. For example the length of a MV cable system can be derived from the individual lengths of the respective subsections. Also in the context of cable events the formulation of derived attributes is feasible, taking into account external events as well as cable placement conditions. Therefore, features such as the number of digging activities affecting one cable system as well as the number of crossing roads of a cable system can be easily derived by using the provided data schema. It should also be emphasised that the presented model design allows the formulation of nested derived attributes, such as the number of failures at the main station level. Further, the number of subsections or joints per MV cable system can be computed on the basis of the existing entity relations.



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
Each node will generate its folder directory for the database in `idss_graph_db`, and will generate its own log file in the `log` directory.

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
