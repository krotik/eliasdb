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
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"devt.de/common/fileutil"
	"devt.de/common/httputil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/cluster/manager"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/storage"
)

const testdb = "testdb"

const invalidFileName = "**" + string(0x0)

var printLog = []string{}
var errorLog = []string{}

var printLogging = false

func TestMain(m *testing.M) {
	flag.Parse()

	basepath = testdb + "/"

	// Log all print and error messagesí

	print = func(v ...interface{}) {
		if printLogging {
			fmt.Println(v...)
		}
		printLog = append(printLog, fmt.Sprint(v...))
	}
	fatal = func(v ...interface{}) {
		if printLogging {
			fmt.Println(v...)
		}
		errorLog = append(errorLog, fmt.Sprint(v...))
	}

	defer func() {
		fatal = log.Fatal
		basepath = ""
	}()

	if res, _ := fileutil.PathExists(testdb); res {
		if err := os.RemoveAll(testdb); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
	}

	ensurePath(testdb)

	// Run the tests

	res := m.Run()

	if res, _ := fileutil.PathExists(testdb); res {
		if err := os.RemoveAll(testdb); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
	}

	os.Exit(res)
}

func TestImportExportError(t *testing.T) {

	ioutil.WriteFile("invalid_import.json", []byte(`
{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X",
`), 0644)
	defer func() {
		if err := os.Remove("invalid_import.json"); err != nil {
			fmt.Print("Could not remove test import file:", err.Error())
		}
	}()

	// Test error cases

	out, _ := execMain([]string{"eliasdb", "-import", "bla.txt"})
	if out != `
Please specify a partition to import to
`[1:] {
		t.Error("Unexpected output:", out)
		return
	}

	out, _ = execMain([]string{"eliasdb", "-import", "bla.txt"})
	if out != `
Please specify a partition to import to
`[1:] {
		t.Error("Unexpected output:", out)
		return
	}

	out, _ = execMain([]string{"eliasdb", "-part", "main", "-import", invalidFileName})
	if !strings.HasPrefix(out, "Could not read from import file") {
		t.Error("Unexpected output:", out)
		return
	}

	out, _ = execMain([]string{"eliasdb", "-part", "main", "-import", "invalid_import.json"})
	if !strings.HasPrefix(out, "Could not import graph") {
		t.Error("Unexpected output:", out)
		return
	}

	out, _ = execMain([]string{"eliasdb", "-dumpdb", "bla.txt"})
	if out != `
Please specify a partition to dump
`[1:] {
		t.Error("Unexpected output:", out)
		return
	}

	DefaultConfig[MemoryOnlyStorage] = true
	out, _ = execMain([]string{"eliasdb", "-part", "main", "-dumpdb", "bla.txt"})
	if out != `
Nothing to dump from a memory only datastore
`[1:] {
		t.Error("Unexpected output:", out)
		return
	}
	DefaultConfig[MemoryOnlyStorage] = false

	out, _ = execMain([]string{"eliasdb", "-part", "main", "-dumpdb", invalidFileName})
	if !strings.HasPrefix(out, "Could not dump graph") {
		t.Error("Unexpected output:", out)
		return
	}

	gs := graphstorage.NewMemoryGraphStorage("test")
	gm := graph.NewGraphManager(gs)

	// Export an empty graph

	err := handleJSONExport(gm, "main", "test_export.json")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		if err := os.Remove("test_export.json"); err != nil {
			fmt.Print("Could not remove test import file:", err.Error())
		}
	}()

	xout, err := ioutil.ReadFile("test_export.json")
	if err != nil {
		t.Error(err)
	}

	if string(xout) != `
{
  "nodes" : [
  ],
  "edges" : [
  ]
}
`[1:] {
		t.Error("Unexpected output of empty graph:", string(xout))
		return
	}

	// Try exporting nodes with unexportable attibutes

	err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "123",
		"kind": "bla",
		"test": data.NewGraphNode,
	}))

	if err != nil {
		t.Error(err)
		return
	}

	err = handleJSONExport(gm, "main", "test_export.json")
	if err != nil {
		t.Error(err)
		return
	}
	xout, err = ioutil.ReadFile("test_export.json")
	if !strings.Contains(string(xout), `"test" : null`) {
		t.Error("Unexpected output:", string(xout))
		return
	}

	// Error when reading a node

	msm := gs.StorageManager("main"+"bla"+graph.StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	msm.AccessMap[1] = storage.AccessCacheAndFetchSeriousError

	err = handleJSONExport(gm, "main", "test_export.json")
	if !strings.HasPrefix(err.Error(), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 1)

	err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "456",
		"kind": "bla",
		"test": data.NewGraphNode,
	}))

	msm = gs.StorageManager("main"+"bla"+graph.StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	msm.AccessMap[6] = storage.AccessCacheAndFetchSeriousError

	err = handleJSONExport(gm, "main", "test_export.json")
	if !strings.HasPrefix(err.Error(), "GraphError: Could not read graph information") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 6)

	msm.AccessMap[5] = storage.AccessCacheAndFetchSeriousError

	err = handleJSONExport(gm, "main", "test_export.json")
	if !strings.HasPrefix(err.Error(), "GraphError: Could not read graph information") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 5)

	gm.StoreEdge("main", data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(map[string]interface{}{
		"end1cascading": false,
		"end1key":       "123",
		"end1kind":      "bla",
		"end1role":      "node",
		"end2cascading": false,
		"end2key":       "456",
		"end2kind":      "bla",
		"end2role":      "node",
		"key":           "3",
		"kind":          "xxx",
	})))

	// Traverse to relationship should fail

	msm.AccessMap[7] = storage.AccessCacheAndFetchSeriousError

	err = handleJSONExport(gm, "main", "test_export.json")
	if !strings.HasPrefix(err.Error(), "GraphError: Could not read graph information") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 7)

	// Lookup of relationship should fail

	msm = gs.StorageManager("main"+"xxx"+graph.StorageSuffixEdges, false).(*storage.MemoryStorageManager)

	msm.AccessMap[1] = storage.AccessCacheAndFetchSeriousError

	err = handleJSONExport(gm, "main", "test_export.json")
	if !strings.HasPrefix(err.Error(), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 1)

	// Test invalid import data

	xout = []byte(`
	{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X"
	    },
	    {
	      "key": "2"
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "4",
	      "kind": "A"
	    }		
	]
}`)

	// Now try the import again

	err = handleJSONImport(gm, "main", xout)
	if err.Error() != "GraphError: Invalid data (Node is missing a kind value)" {
		t.Error("Unexpected graph error:", err)
		return
	}

	xout = []byte(`
	{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X"
	    },
	    {
	      "key": "2",
	      "kind": "Y"
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "4",
	      "kind": "A"
	    }		
	]
}`)

	// Now try the import again

	err = handleJSONImport(gm, "main", xout)
	if err.Error() != "GraphError: Invalid data (Edge is missing a cascading value for end2)" {
		t.Error("Unexpected graph error:", err)
		return
	}
}

func TestCommandLineParameter(t *testing.T) {

	// Test normal usage

	out, _ := execMain([]string{"eliasdb", "-?"})
	if out != `
Usage of  eliasdb  [options]
  -?	Show this help message
  -dumpdb string
    	Dump the contents of a partition to a JSON file and exit
  -import string
    	Import a graph from a JSON file to a partition (exit if storing on disk)
  -part string
    	Partition to operate on when importing or dumping data
`[1:] {
		t.Error("Unexpected usage text:", out)
		return
	}

	// Test import / export

	testImportFile := "test_import.json"

	ioutil.WriteFile(testImportFile, []byte(`
{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X",
	      "name": "Test1"
	    },
	    {
	      "key": "2",
	      "kind": "Y",
	      "name": "Test2",
		  "test1": true,
		  "test2": 123,
		  "test3": 22E-11
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "3",
	      "kind": "Z"
	    },
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "4",
	      "kind": "A"
	    }		
	]
}
`[1:]), 0644)
	defer func() {
		if err := os.Remove(testImportFile); err != nil {
			fmt.Print("Could not remove test import file:", err.Error())
		}
	}()

	// Test data import

	out, _ = execMain([]string{"eliasdb", "-part", "importexport", "-import", "test_import.json"})
	if out != "" {
		t.Error("Unexpected error output:", out)
		return
	}

	// Test data export

	out, _ = execMain([]string{"eliasdb", "-part", "importexport", "-dumpdb", "test_export.json"})

	if out != "" {
		t.Error("Unexpected error output:", out)
		return
	}

	defer func() {
		if err := os.Remove("test_export.json"); err != nil {
			fmt.Print("Could not remove test import file:", err.Error())
		}
	}()

	// Check that the exported file is correct

	exportString, err := ioutil.ReadFile("test_export.json")
	if err != nil {
		t.Error(err)
		return
	}

	var filedata map[string][]map[string]interface{}

	dec := json.NewDecoder(bytes.NewBuffer(exportString))

	if err := dec.Decode(&filedata); err != nil {
		t.Error("Could not decode file content as object with list of nodes and edges:", err.Error())
		return
	}

	if len(filedata["nodes"]) != 2 || len(filedata["edges"]) != 2 {
		t.Error("Unexpected lengths of export data")
		return
	}

	obj := make(map[string]map[string]interface{})

	for _, x := range filedata["nodes"] {
		obj[x["key"].(string)] = x
	}
	for _, x := range filedata["edges"] {
		obj[x["key"].(string)] = x
	}

	if len(obj["1"]) != 3 {
		t.Error("Unexpected number of attributes:", obj["1"])
		return
	}
	if len(obj["2"]) != 6 {
		t.Error("Unexpected number of attributes:", obj["2"])
		return
	}
	if len(obj["3"]) != 10 {
		t.Error("Unexpected number of attributes:", obj["3"])
		return
	}
	if len(obj["4"]) != 10 {
		t.Error("Unexpected number of attributes:", obj["4"])
		return
	}

	// Test correct types have been imported

	if obj["2"]["test1"] != true {
		t.Errorf("Unexpected type: %v (%T)", obj["2"]["test1"], obj["2"]["test1"])
		return
	}
	if obj["2"]["test2"] != float64(123) {
		t.Errorf("Unexpected type: %v (%T)", obj["2"]["test2"], obj["2"]["test2"])
		return
	}
	if obj["2"]["test3"] != 2.2e-10 {
		t.Errorf("Unexpected type: %v (%T)", obj["2"]["test3"], obj["2"]["test3"])
		return
	}

}

func TestMainNormalCase(t *testing.T) {

	// Make sure to reset the DefaultServeMux

	defer func() { http.DefaultServeMux = http.NewServeMux() }()

	// Make sure to remove any files

	defer func() {
		if err := os.RemoveAll(testdb); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
		ensurePath(testdb)
	}()

	// Reset logs

	printLog = []string{}
	errorLog = []string{}

	errorChan := make(chan error)

	// Test handle function

	api.HandleFunc = func(pattern string, handler func(http.ResponseWriter, *http.Request)) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Request with nil request and nil response should cause a panic.")
			}
		}()
		handler(nil, nil)
	}

	// Start cluster by default

	Config[EnableCluster] = true
	Config[EnableClusterTerminal] = true

	// Kick off main function

	go func() {
		out, _ := execMain(nil)

		Config[EnableCluster] = false
		Config[EnableClusterTerminal] = false

		lines := strings.Split(strings.TrimSpace(out), "\n")

		errorChan <- nil

		// stderr should contain one line from the rpc code

		if len(lines) != 1 || !strings.Contains(lines[0], "rpc.Serve: accept") {
			t.Error("Unexpected stderr:", out)
			return
		}
	}()

	// To exit the main function the lock watcher thread
	// has to recognise that the lockfile was modified

	time.Sleep(time.Duration(2) * time.Second)

	// Do a normal shutdown with a log file

	if err := shutdownWithLogFile(); err != nil {
		t.Error(err)
		return
	}

	// Wait for the main function to end

	if err := <-errorChan; err != nil || len(errorLog) != 0 {
		t.Error("Unexpected ending of main thread:", err, errorLog)
		return
	}

	// Check the print log

	if logString := strings.Join(printLog, "\n"); logString != `
EliasDB 0.8.0
Starting datastore in testdb/db
Reading cluster config
Opening cluster state info
Starting cluster (log history: 100)
[Cluster] member1: Starting member manager member1 rpc server on: localhost:9030
Creating GraphManager instance
Creating key (key.pem) and certificate (cert.pem) in: ssl
Ensuring web folder: testdb/web
Ensuring web termminal: testdb/web/db/term.html
Ensuring cluster termminal: testdb/web/db/cluster.html
Starting server on: localhost:9090
Writing fingerprint file: testdb/web/fingerprint.json
Waiting for shutdown
Lockfile was modified
Shutting down
[Cluster] member1: Housekeeping stopped
[Cluster] member1: Shutdown rpc server on: localhost:9030
[Cluster] member1: Connection closed: localhost:9030
Closing datastore`[1:] {
		t.Error("Unexpected log:", logString)
		return
	}
}

func TestMainErrorCases(t *testing.T) {

	// Make sure to reset the DefaultServeMux

	defer func() { http.DefaultServeMux = http.NewServeMux() }()

	// Make sure to remove any files

	defer func() {
		if err := os.RemoveAll(testdb); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
		ensurePath(testdb)
	}()

	// Setup config and logs

	Config = nil
	origConfFile := ConfigFile
	origBasePath := basepath

	basepath = ""
	ConfigFile = invalidFileName

	printLog = []string{}
	errorLog = []string{}

	execMain(nil)

	// Check that an error happened

	if len(errorLog) != 1 || (errorLog[0] != "stat **"+string(0)+": invalid argument" &&
		errorLog[0] != "Lstat **"+string(0)+": invalid argument") {

		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back variables

	basepath = origBasePath
	ConfigFile = origConfFile
	printLog = []string{}
	errorLog = []string{}

	data := make(map[string]interface{})
	for k, v := range DefaultConfig {
		data[k] = v
	}

	Config = data

	// Test db access error

	Config[LocationDatastore] = invalidFileName
	Config[EnableReadOnly] = true

	execMain(nil)

	// Check that an error happened

	if len(errorLog) != 2 ||
		!strings.Contains(errorLog[0], "Could not create directory") ||
		!strings.Contains(errorLog[1], "Failed to open graph storage") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	// Use memory only storage and the ignored readonly flag

	Config[MemoryOnlyStorage] = true
	Config[EnableReadOnly] = true

	// Test failed ssl key generation

	Config[HTTPSKey] = invalidFileName

	execMain(nil)

	// Check that an error happened

	if len(errorLog) != 1 ||
		!strings.Contains(errorLog[0], "Failed to generate ssl key and certificate") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	Config[HTTPSKey] = DefaultConfig[HTTPSKey]

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	// Special error when closing the store

	graphstorage.MgsRetClose = errors.New("Testerror")

	// Use 9090

	Config[HTTPSPort] = "9090"

	ths := httputil.HTTPServer{}
	go ths.RunHTTPServer(":9090", nil)

	time.Sleep(time.Duration(1) * time.Second)

	execMain(nil)

	ths.Shutdown()

	time.Sleep(time.Duration(1) * time.Second)

	if ths.Running {
		t.Error("Server should not be running")
		return
	}

	if len(errorLog) != 2 || (errorLog[0] != "listen tcp :9090"+
		": bind: address already in use" && errorLog[0] != "listen tcp :9090"+
		": bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.") ||
		errorLog[1] != "Testerror" {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	Config[EnableCluster] = true

	Config[ClusterStateInfoFile] = invalidFileName

	execMain(nil)

	if len(errorLog) != 1 ||
		!strings.Contains(errorLog[0], "Failed to load cluster state info") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	Config[ClusterConfigFile] = invalidFileName

	execMain(nil)

	if len(errorLog) != 1 ||
		!strings.Contains(errorLog[0], "Failed to load cluster config") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Call to debug log function

	manager.LogDebug("test debug text")
	if logout := api.DDLog.String(); !strings.Contains(logout, "test debug text") {
		t.Error("Unexpected error:", logout)
		return
	}

	Config = nil
}

func shutdownWithLogFile() error {
	file, err := os.OpenFile(basepath+config(LockFile), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	defer file.Close()
	if err != nil {
		fmt.Println(errorLog)
		return err
	}

	_, err = file.Write([]byte("a"))
	if err != nil {
		return err
	}

	return nil
}

/*
Execute the main function and capture the output.
*/
func execMain(args []string) (string, error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Main execution caused a panic.")
			out, err := ioutil.ReadFile("out.txt")
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(out)
		}
	}()

	// Exchange stderr to a file

	origStdErr := os.Stderr

	outFile, err := os.Create("out.txt")
	if err != nil {
		return "", err
	}
	defer func() {
		outFile.Close()
		os.RemoveAll("out.txt")

		// Put Stderr back

		os.Stderr = origStdErr
		log.SetOutput(os.Stderr)
	}()

	os.Stderr = outFile
	log.SetOutput(outFile)

	if args == nil {
		args = []string{"eliasdb"}
	}
	os.Args = args
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	main()

	// Reset flags

	flag.CommandLine = &flag.FlagSet{}

	outFile.Sync()

	out, err := ioutil.ReadFile("out.txt")
	if err != nil {
		return "", err
	}

	return string(out), nil
}
