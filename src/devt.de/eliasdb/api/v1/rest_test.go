/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package v1

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"devt.de/common/httputil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/eql"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
)

const TESTPORT = ":9090"

var gmMSM *graphstorage.MemoryGraphStorage

// Main function for all tests in this package

func TestMain(m *testing.M) {
	flag.Parse()

	gm, msm := songGraph()
	api.GM = gm
	gmMSM = msm

	hs, wg := startServer()
	if hs == nil {
		return
	}

	// Register endpoints for version 1

	api.RegisterRestEndpoints(V1EndpointMap)

	// Run the tests

	res := m.Run()

	// Teardown

	stopServer(hs, wg)

	os.Exit(res)
}

func TestSwaggerDefs(t *testing.T) {

	// Test we can build swagger defs from the endpoint

	data := map[string]interface{}{
		"paths":       map[string]interface{}{},
		"definitions": map[string]interface{}{},
	}

	for _, inst := range V1EndpointMap {
		inst().SwaggerDefs(data)
	}
}

/*
Send a request to a HTTP test server
*/
func sendTestRequest(url string, method string, content []byte) (string, http.Header, string) {
	var req *http.Request
	var err error

	if content != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(content))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	bodyStr := strings.Trim(string(body), " \n")

	// Try json decoding first

	out := bytes.Buffer{}
	err = json.Indent(&out, []byte(bodyStr), "", "  ")
	if err == nil {
		return resp.Status, resp.Header, out.String()
	}

	// Just return the body

	return resp.Status, resp.Header, bodyStr
}

/*
Start a HTTP test server.
*/
func startServer() (*httputil.HTTPServer, *sync.WaitGroup) {
	hs := &httputil.HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	go hs.RunHTTPServer(TESTPORT, &wg)

	wg.Wait()

	// Server is started

	if hs.LastError != nil {
		panic(hs.LastError)
	}

	return hs, &wg
}

/*
Stop a started HTTP test server.
*/
func stopServer(hs *httputil.HTTPServer, wg *sync.WaitGroup) {

	if hs.Running == true {

		wg.Add(1)

		// Server is shut down

		hs.Shutdown()

		wg.Wait()

	} else {

		panic("Server was not running as expected")
	}
}

func songGraph() (*graph.GraphManager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	constructEdge := func(key string, node1 data.Node, node2 data.Node, number int) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "Wrote")

		edge.SetAttr(data.EDGE_END1_KEY, node1.Key())
		edge.SetAttr(data.EDGE_END1_KIND, node1.Kind())
		edge.SetAttr(data.EDGE_END1_ROLE, "Author")
		edge.SetAttr(data.EDGE_END1_CASCADING, true)

		edge.SetAttr(data.EDGE_END2_KEY, node2.Key())
		edge.SetAttr(data.EDGE_END2_KIND, node2.Kind())
		edge.SetAttr(data.EDGE_END2_ROLE, "Song")
		edge.SetAttr(data.EDGE_END2_CASCADING, false)

		edge.SetAttr("number", number)

		return edge
	}

	storeSong := func(node data.Node, name string, ranking int, number int) {
		node3 := data.NewGraphNode()
		node3.SetAttr("key", name)
		node3.SetAttr("kind", "Song")
		node3.SetAttr("name", name)
		node3.SetAttr("ranking", ranking)
		gm.StoreNode("main", node3)
		gm.StoreEdge("main", constructEdge(name, node, node3, number))
	}

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "Author")
	node0.SetAttr("name", "John")
	gm.StoreNode("main", node0)

	storeSong(node0, "Aria1", 8, 1)
	storeSong(node0, "Aria2", 2, 2)
	storeSong(node0, "Aria3", 4, 3)
	storeSong(node0, "Aria4", 18, 4)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "Author")
	node1.SetAttr("name", "Mike")
	gm.StoreNode("main", node1)

	storeSong(node1, "LoveSong3", 1, 3)
	storeSong(node1, "FightSong4", 3, 4)
	storeSong(node1, "DeadSong2", 6, 2)
	storeSong(node1, "StrangeSong1", 5, 1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "Author")
	node2.SetAttr("name", "Hans")
	gm.StoreNode("main", node2)

	storeSong(node2, "MyOnlySong3", 19, 3)

	// Create lots of spam nodes

	for i := 0; i < 21; i++ {
		nodespam := data.NewGraphNode()
		nodespam.SetAttr("key", "000"+strconv.Itoa(i))
		nodespam.SetAttr("kind", "Spam")
		nodespam.SetAttr("name", "Spam"+strconv.Itoa(i))
		gm.StoreNode("main", nodespam)
	}

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}

func songGraphGroups() (*graph.GraphManager, *graphstorage.MemoryGraphStorage) {
	gm, mgs := songGraph()

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "Best")
	node0.SetAttr("kind", eql.GROUP_NODE_KIND)
	gm.StoreNode("main", node0)

	constructEdge := func(songkey string) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", songkey)
		edge.SetAttr("kind", "Contains")

		edge.SetAttr(data.EDGE_END1_KEY, node0.Key())
		edge.SetAttr(data.EDGE_END1_KIND, node0.Kind())
		edge.SetAttr(data.EDGE_END1_ROLE, "group")
		edge.SetAttr(data.EDGE_END1_CASCADING, false)

		edge.SetAttr(data.EDGE_END2_KEY, songkey)
		edge.SetAttr(data.EDGE_END2_KIND, "Song")
		edge.SetAttr(data.EDGE_END2_ROLE, "Song")
		edge.SetAttr(data.EDGE_END2_CASCADING, false)

		return edge
	}

	gm.StoreEdge("main", constructEdge("LoveSong3"))
	gm.StoreEdge("main", constructEdge("Aria3"))
	gm.StoreEdge("main", constructEdge("MyOnlySong3"))
	gm.StoreEdge("main", constructEdge("StrangeSong1"))

	return gm, mgs
}
