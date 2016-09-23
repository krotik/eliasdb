/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"devt.de/common/fileutil"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
)

/*
config reads a config value as string value.
*/
func config(key string) string {
	return fmt.Sprint(Config[key])
}

/*
ensurePath ensures that a given relative path exists.
*/
func ensurePath(path string) {
	if res, _ := fileutil.PathExists(path); !res {
		if err := os.Mkdir(path, 0770); err != nil {
			fatal("Could not create directory:", err.Error())
			return
		}
	}
}

/*
handleCommandLine handles all command line options
*/
func handleCommandLine(gm *graph.Manager) bool {

	importFile := flag.String("import", "", "Import a graph from a JSON file to a partition (exit if storing on disk)")
	exportFile := flag.String("dumpdb", "", "Dump the contents of a partition to a JSON file and exit")
	part := flag.String("part", "", "Partition to operate on when importing or dumping data")
	showHelp := flag.Bool("?", false, "Show this help message")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [options]")
		flag.PrintDefaults()
		return
	}

	flag.Parse()

	if *showHelp || (*part != "" && *importFile == "" && *exportFile == "") {
		flag.Usage()
		return false
	} else if *importFile != "" {

		// Check that we have a partition to operate on

		if *part == "" {
			fmt.Fprintln(os.Stderr, "Please specify a partition to import to")
			return false
		}

		print("Importing from ", *importFile)

		// On a successful import continue if we have a memory only datastore

		importString, err := ioutil.ReadFile(*importFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not read from import file: ", err)
			return false
		}

		err = handleJSONImport(gm, *part, importString)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not import graph: ", err)
			return false
		}

		return Config[MemoryOnlyStorage].(bool)

	} else if *exportFile != "" {

		// Check that we have a partition to dump and are not running memory only

		if *part == "" {
			fmt.Fprintln(os.Stderr, "Please specify a partition to dump")
			return false
		} else if Config[MemoryOnlyStorage].(bool) {
			fmt.Fprintln(os.Stderr, "Nothing to dump from a memory only datastore")
			return false
		}

		print("Dumping into ", *exportFile)

		err := handleJSONExport(gm, *part, *exportFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not dump graph: ", err)
			return false
		}

		return false
	}

	return true
}

/*
handleJSONExport dumps the contents of a partition to a JSON file. The graph should have the
following format:

	{
		nodes : [ { <attr> : <value> }, ... ]
		edges : [ { <attr> : <value> }, ... ]
	}
*/
func handleJSONExport(gm *graph.Manager, part string, filename string) error {

	// Use a map to unique found edge keys

	edgeKeys := make(map[string]string)

	// Open output file

	outFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer outFile.Close()

	writeData := func(data map[string]interface{}) {

		nk := 0
		for k, v := range data {

			// JSON encode value - ignore values which cannot be JSON encoded

			jv, err := json.Marshal(v)

			// Encoding errors result in a null value

			if err != nil {
				jv = []byte("null")
			}

			// Write out the node attributes

			outFile.WriteString(fmt.Sprintf("      \"%s\" : %s", k, jv))
			if nk < len(data)-1 {
				outFile.WriteString(",")
			}
			outFile.WriteString("\n")
			nk++
		}
	}

	// Iterate over all available node kinds

	outFile.WriteString(`{
  "nodes" : [
`)

	kinds := gm.NodeKinds()
	for ik, kind := range kinds {

		// Loop over all available kinds

		it, err := gm.NodeKeyIterator(part, kind)
		if err != nil {
			return err
		}

		// Iterate over all node keys

		for i := 0; it.HasNext(); i++ {
			key := it.Next()

			if it.LastError != nil {
				return it.LastError
			}

			node, err := gm.FetchNode(part, key, kind)
			if err != nil {
				return err
			}

			// Fetch all connected relationships and store their key and kind

			_, edges, err := gm.TraverseMulti(part, key, kind, ":::", false)
			if err != nil {
				return err
			}

			for _, edge := range edges {
				edgeKeys[edge.Kind()+edge.Key()] = edge.Kind()
			}

			// Write out JSON object

			outFile.WriteString("    {\n")

			writeData(node.Data())

			if it.HasNext() || ik < len(kinds)-1 {
				outFile.WriteString("    },\n")
			} else {
				outFile.WriteString("    }\n")
			}
		}
	}

	outFile.WriteString(`  ],
  "edges" : [
`)

	// Iterate over all available edge kinds

	ie := 0
	for key, kind := range edgeKeys {
		key = key[len(kind):]

		edge, err := gm.FetchEdge(part, key, kind)
		if err != nil {
			return err
		}

		// Write out JSON object

		outFile.WriteString("    {\n")

		writeData(edge.Data())

		if ie < len(edgeKeys)-1 {
			outFile.WriteString("    },\n")
		} else {
			outFile.WriteString("    }\n")
		}

		ie++
	}

	outFile.WriteString(`  ]
}
`)

	return nil
}

/*
handleJSONImport imports a graph from a JSON string. The graph should have the
following format:

	{
		nodes : [ { <attr> : <value> }, ... ]
		edges : [ { <attr> : <value> }, ... ]
	}
*/
func handleJSONImport(gm *graph.Manager, part string, stringData []byte) error {

	dec := json.NewDecoder(bytes.NewBuffer(stringData))
	gdata := make(map[string][]map[string]interface{})

	if err := dec.Decode(&gdata); err != nil {
		return fmt.Errorf("Could not decode file content as object with list of nodes and edges: %s", err.Error())
	}

	nDataList := gdata["nodes"]
	eDataList := gdata["edges"]

	// Create a transaction

	trans := graph.NewGraphTrans(gm)

	// Store nodes in transaction

	for _, ndata := range nDataList {
		node := data.NewGraphNodeFromMap(ndata)

		if err := trans.StoreNode(part, node); err != nil {
			return err
		}
	}

	// Store edges in transaction

	for _, edata := range eDataList {
		edge := data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(edata))

		if err := trans.StoreEdge(part, edge); err != nil {
			return err
		}
	}

	// Commit transaction

	return trans.Commit()
}
