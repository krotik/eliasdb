/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"devt.de/common/errorutil"
	"devt.de/eliasdb/graph/data"
)

/*
ExportPartition dumps the contents of a partition to an io.Writer in JSON format:

	{
		nodes : [ { <attr> : <value> }, ... ]
		edges : [ { <attr> : <value> }, ... ]
	}
*/
func ExportPartition(out io.Writer, part string, gm *Manager) error {

	// Use a map to unique found edge keys

	edgeKeys := make(map[string]string)

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

			fmt.Fprintf(out, "      \"%s\" : %s", k, jv)
			if nk < len(data)-1 {
				fmt.Fprint(out, ",")
			}
			fmt.Fprint(out, "\n")
			nk++
		}
	}

	// Iterate over all available node kinds

	fmt.Fprint(out, `{
  "nodes" : [
`)

	// Loop over all available kinds and build iterators if nodes
	// exist in the given partition

	var iters []*NodeKeyIterator
	var kinds []string

	for _, k := range gm.NodeKinds() {

		it, err := gm.NodeKeyIterator(part, k)
		if err != nil {
			return err
		}
		if it != nil {
			iters = append(iters, it)
			kinds = append(kinds, k)
		}
	}

	for ik, it := range iters {

		// Iterate over all node keys

		for i := 0; it.HasNext(); i++ {
			key := it.Next()

			if it.LastError != nil {
				return it.LastError
			}

			node, err := gm.FetchNode(part, key, kinds[ik])
			if err != nil {
				return err
			}

			// Fetch all connected relationships and store their key and kind

			_, edges, err := gm.TraverseMulti(part, key, kinds[ik], ":::", false)
			if err != nil {
				return err
			}

			for _, edge := range edges {
				edgeKeys[edge.Kind()+edge.Key()] = edge.Kind()
			}

			// Write out JSON object

			fmt.Fprint(out, "    {\n")

			writeData(node.Data())

			if it.HasNext() || ik < len(iters)-1 {
				fmt.Fprint(out, "    },\n")
			} else {
				fmt.Fprint(out, "    }\n")
			}
		}
	}

	fmt.Fprint(out, `  ],
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

		fmt.Fprint(out, "    {\n")

		writeData(edge.Data())

		if ie < len(edgeKeys)-1 {
			fmt.Fprint(out, "    },\n")
		} else {
			fmt.Fprint(out, "    }\n")
		}

		ie++
	}

	fmt.Fprint(out, `  ]
}`)

	return nil
}

/*
SortDump sorts a string result which was produced by ExportPartition.
Do not use this for very large results. Panics if the input data is not valid.
*/
func SortDump(in string) string {
	var nodes []data.Node
	var edges []data.Node

	dec := json.NewDecoder(bytes.NewBufferString(in))
	gdata := make(map[string][]map[string]interface{})

	errorutil.AssertOk(dec.Decode(&gdata))

	nDataList := gdata["nodes"]
	for _, n := range nDataList {
		nodes = append(nodes, data.NewGraphNodeFromMap(n))
	}
	data.NodeSort(nodes)
	for i, n := range nodes {
		nDataList[i] = n.Data()
	}

	eDataList := gdata["edges"]
	for _, n := range eDataList {
		edges = append(edges, data.NewGraphNodeFromMap(n))
	}
	data.NodeSort(edges)
	for i, e := range edges {
		eDataList[i] = e.Data()
	}

	res, err := json.MarshalIndent(map[string]interface{}{
		"nodes": nDataList,
		"edges": eDataList,
	}, "", "    ")

	errorutil.AssertOk(err)

	return string(res)
}

/*
ImportPartition imports the JSON contents of an io.Reader into a given partition.
The following format is expected:

	{
		nodes : [ { <attr> : <value> }, ... ]
		edges : [ { <attr> : <value> }, ... ]
	}
*/
func ImportPartition(in io.Reader, part string, gm *Manager) error {

	dec := json.NewDecoder(in)
	gdata := make(map[string][]map[string]interface{})

	if err := dec.Decode(&gdata); err != nil {
		return fmt.Errorf("Could not decode file content as object with list of nodes and edges: %s", err.Error())
	}

	nDataList := gdata["nodes"]
	eDataList := gdata["edges"]

	// Create a transaction

	trans := NewGraphTrans(gm)

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
