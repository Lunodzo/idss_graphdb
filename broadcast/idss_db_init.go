/*
* File: broadcast/idss_db_init.go
* Copyright 2023-2027, University of Salento, Italy.
* All rights reserved.
*
* This file contains functions to generate demo data and initialize the EliasDB graph
* database by loading the generated data. It also initializes a Query Manager node for
* query metadata. Optimizations include streaming JSON parsing, batched transactions,
* and cleanup of temporary JSON files.
*/

package broadcast

import (
    "encoding/json"
    "os"
    "os/exec"
    "path/filepath"
    "runtime/pprof"
    "time"

    "github.com/krotik/eliasdb/eql"
    "github.com/krotik/eliasdb/graph"
    "github.com/krotik/eliasdb/graph/data"
    "github.com/krotik/eliasdb/graph/graphstorage"
)

// Function to generate fake data and initialize the graph database
func GenFakeDataAndInit(dataFilePath string, dbPath string, graphDB graphstorage.Storage, graphManager *graph.Manager) error {
    // Execute python script to generate fake data
    err := generateData(dataFilePath)
    if err != nil {
        logger.Fatal("Failed to generate data: ", err)
        return err
    }

    logger.Info("Data generation completed, now loading data into the graph database...")

    // Load the data into the graph database
    err = LoadGraphData(dataFilePath, dbPath, graphDB, graphManager)
    if err != nil {
        logger.Error("Failed to load data into graph database: ", err)
        return err
    }

    // Delete the JSON file after successful loading
    err = os.Remove(dataFilePath)
    if err != nil {
        logger.Warn("Failed to delete JSON file ", dataFilePath, ": ", err)
    } else {
        logger.Info("Successfully deleted JSON file: ", dataFilePath)
    }

    return nil
}

// Function to generate fake data using a python script
func generateData(dataFilePath string) error {
    // Ensure the directory exists
    if err := os.MkdirAll(filepath.Dir(dataFilePath), os.ModePerm); err != nil {
        logger.Error("Failed to create directory for JSON file: ", err)
        return err
    }

    // Pass configurable parameters (adjust as needed)
    cmd := exec.Command("python3", "generate_data.py", dataFilePath,
        "--num-clients", "1",
        "--num-consumptions", "1",
        "--edges-per-client", "1")
    err := cmd.Run()
    if err != nil {
        logger.Fatal("Data generation command execution failed: ", err)
        return err
    }
    return nil
}

// Function to load graph data into the graph database using a JSON file
func LoadGraphData(dataFilePath string, dbPath string, graphDB graphstorage.Storage, graphManager *graph.Manager) error {
    // Take heap snapshot before initialization
    f, err := os.Create("heap_before_init.pprof")
    if err == nil {
        pprof.WriteHeapProfile(f)
        f.Close()
    } else {
        logger.Warn("Failed to create heap profile: ", err)
    }

    // Open the JSON file
    file, err := os.Open(dataFilePath)
    if err != nil {
        logger.Error("Error opening JSON file: ", err)
        return err
    }
    defer file.Close()

    const batchSize = 1000
    trans := graph.NewGraphTrans(graphManager)
    nodeCount, edgeCount := 0, 0

    // Stream JSON data
    decoder := json.NewDecoder(file)
    _, err = decoder.Token() // Consume opening '{'
    if err != nil {
        logger.Error("Error parsing JSON: ", err)
        return err
    }

    for decoder.More() {
        token, err := decoder.Token()
        if err != nil {
            logger.Error("Error reading JSON token: ", err)
            return err
        }
        key := token.(string)

        if key == "nodes" {
            _, err = decoder.Token() // Consume '['
            if err != nil {
                return err
            }
            for decoder.More() {
                var nodeData map[string]interface{}
                if err := decoder.Decode(&nodeData); err != nil {
                    logger.Error("Error decoding node: ", err)
                    return err
                }
                graphNode := data.NewGraphNode()
                for k, v := range nodeData {
                    graphNode.SetAttr(k, v)
                }
                trans.StoreNode("main", graphNode)
                nodeCount++
                if nodeCount%batchSize == 0 {
                    if err := trans.Commit(); err != nil {
                        logger.Error("Error committing node batch: ", err)
                        return err
                    }
                    trans = graph.NewGraphTrans(graphManager)
                }
            }
            _, err = decoder.Token() // Consume ']'
            if err != nil {
                return err
            }
        } else if key == "edges" {
            _, err = decoder.Token() // Consume '['
            if err != nil {
                return err
            }
            for decoder.More() {
                var edgeData map[string]interface{}
                if err := decoder.Decode(&edgeData); err != nil {
                    logger.Error("Error decoding edge: ", err)
                    return err
                }
                graphEdge := data.NewGraphEdge()
                for k, v := range edgeData {
                    graphEdge.SetAttr(k, v)
                }
                trans.StoreEdge("main", graphEdge)
                edgeCount++
                if edgeCount%batchSize == 0 {
                    if err := trans.Commit(); err != nil {
                        logger.Error("Error committing edge batch: ", err)
                        return err
                    }
                    trans = graph.NewGraphTrans(graphManager)
                }
            }
            _, err = decoder.Token() // Consume ']'
            if err != nil {
                return err
            }
        }
    }

    // Commit any remaining nodes/edges
    if nodeCount%batchSize != 0 || edgeCount%batchSize != 0 {
        if err := trans.Commit(); err != nil {
            logger.Error("Error committing final batch: ", err)
            return err
        }
    }

    // Take heap snapshot after initialization
    f, err = os.Create("heap_after_init.pprof")
    if err == nil {
        pprof.WriteHeapProfile(f)
        f.Close()
    } else {
        logger.Warn("Failed to create heap profile: ", err)
    }

    logger.Info("Loaded ", nodeCount, " nodes and ", edgeCount, " edges into graph database")
    return nil
}

// Function to initialize the Query Manager graph node
func QueryManager_init(GRAPH_MANAGER *graph.Manager) {
    // Check if sample node exists
    query := "get Query where key = 'sample'"
    result, err := eql.RunQuery("checkQuery", "main", query, GRAPH_MANAGER)
    if err == nil && len(result.Rows()) > 0 {
        logger.Info("Query Manager node already exists")
        return
    }

    logger.Info("Creating the Query Manager node...")

    trans := graph.NewGraphTrans(GRAPH_MANAGER)
    queryNode := data.NewGraphNode()
    queryNode.SetAttr("key", "sample")
    queryNode.SetAttr("kind", "Query")
    queryNode.SetAttr("name", "Query")
    queryNode.SetAttr("query_string", "sample query")
    queryNode.SetAttr("arrival_time", time.Now().Unix())
    queryNode.SetAttr("ttl", 0)
    queryNode.SetAttr("originator", "sample")
    queryNode.SetAttr("sender_address", "")
    queryNode.SetAttr("state", "new")
    queryNode.SetAttr("result", "")

    trans.StoreNode("main", queryNode)
    if err := trans.Commit(); err != nil {
        logger.Error("Error committing Query Manager transaction: ", err)
        return
    }
    logger.Info("Committed Query Manager node store transaction")
}