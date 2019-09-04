/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package server

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/common/httputil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/api/ac"
	"devt.de/krotik/eliasdb/cluster"
	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/config"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

/*
Flag to enable / disable long running tests.
(Only used for test development - should never be false)
*/
const RunLongRunningTests = true

const testdb = "testdb"

const invalidFileName = "**" + string(0x0)

var printLog = []string{}
var errorLog = []string{}

var printLogging = false

func TestMain(m *testing.M) {
	flag.Parse()

	basepath = testdb + "/"

	// Log all print and error messages

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

func TestMainNormalCase(t *testing.T) {

	if !RunLongRunningTests {
		return
	}

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

	// Load default configuration

	config.LoadDefaultConfig()

	// Start cluster by default

	config.Config[config.EnableCluster] = true
	config.Config[config.EnableClusterTerminal] = true

	// Enable access control

	config.Config[config.EnableAccessControl] = true

	// Kick off main function

	go func() {
		out, _ := runServer()

		config.Config[config.EnableCluster] = false
		config.Config[config.EnableClusterTerminal] = false
		config.Config[config.EnableAccessControl] = false

		ac.ACL.Close()

		os.Remove("access.db")

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

	shutdown := false

	go func() {
		filename := basepath + config.Str(config.LockFile)

		for !shutdown {

			// Do a normal shutdown with a log file - don't check for errors

			shutdownWithLogFile(filename)

			time.Sleep(time.Duration(200) * time.Millisecond)
		}
	}()

	// Wait for the main function to end

	if err := <-errorChan; err != nil || len(errorLog) != 0 {
		t.Error("Unexpected ending of main thread:", err, errorLog)
		return
	}

	shutdown = true

	// Check the print log

	logString := strings.Join(printLog, "\n")

	if runtime.GOOS == "windows" {

		// Very primitive but good enough

		logString = strings.Replace(logString, "\\", "/", -1)
	}

	if logString != `
EliasDB `[1:]+config.ProductVersion+`
Starting datastore in testdb/db
Reading cluster config
Opening cluster state info
Starting cluster (log history: 100)
[Cluster] member1: Starting member manager member1 rpc server on: 127.0.0.1:9030
Creating GraphManager instance
Creating key (key.pem) and certificate (cert.pem) in: ssl
Ensuring web folder: testdb/web
Ensuring login page: testdb/web/login.html
Ensuring web terminal: testdb/web/db/term.html
Ensuring cluster terminal: testdb/web/db/cluster.html
Starting server on: 127.0.0.1:9090
Writing fingerprint file: testdb/web/fingerprint.json
Waiting for shutdown
Lockfile was modified
Shutting down
[Cluster] member1: Housekeeping stopped
[Cluster] member1: Shutdown rpc server on: 127.0.0.1:9030
[Cluster] member1: Connection closed: 127.0.0.1:9030
Closing datastore` {
		t.Error("Unexpected log:", logString)
		return
	}
}

func TestMainErrorCases(t *testing.T) {

	if !RunLongRunningTests {
		return
	}

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

	data := make(map[string]interface{})
	for k, v := range config.DefaultConfig {
		data[k] = v
	}

	config.Config = data

	printLog = []string{}
	errorLog = []string{}

	// Test db access error

	config.Config[config.LocationDatastore] = invalidFileName
	config.Config[config.EnableReadOnly] = true

	runServer()

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

	config.Config[config.MemoryOnlyStorage] = true
	config.Config[config.EnableReadOnly] = true

	// Test failed ssl key generation

	config.Config[config.HTTPSKey] = invalidFileName

	runServer()

	// Check that an error happened

	if len(errorLog) != 1 ||
		!strings.Contains(errorLog[0], "Failed to generate ssl key and certificate") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	config.Config[config.HTTPSKey] = config.DefaultConfig[config.HTTPSKey]

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	// Special error when closing the store

	graphstorage.MgsRetClose = errors.New("Testerror")

	// Use 9090

	config.Config[config.HTTPSPort] = "9090"

	ths := httputil.HTTPServer{}
	go ths.RunHTTPServer(":9090", nil)

	time.Sleep(time.Duration(1) * time.Second)

	runServer()

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

	config.Config[config.EnableCluster] = true

	cluster.DSRetNew = errors.New("testerror")
	defer func() {
		cluster.DSRetNew = nil
	}()

	runServer()

	if len(errorLog) != 1 ||
		!strings.Contains(errorLog[0], "testerror") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	config.Config[config.EnableCluster] = true

	config.Config[config.ClusterStateInfoFile] = invalidFileName

	runServer()

	if len(errorLog) != 1 ||
		!strings.Contains(errorLog[0], "Failed to load cluster state info") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	config.Config[config.ClusterConfigFile] = invalidFileName

	runServer()

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

	config.Config = nil

	SOPExecuted := false

	// Test single operation

	StartServerWithSingleOp(func(gm *graph.Manager) bool {
		SOPExecuted = true
		return true
	})

	if !SOPExecuted {
		t.Error("Single operation function was not executed")
		return
	}

	config.Config = nil
}

func shutdownWithLogFile(filename string) error {

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
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
Run the server and capture the output.
*/
func runServer() (string, error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server execution caused a panic.")
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

	StartServer()

	// Reset flags

	outFile.Sync()

	out, err := ioutil.ReadFile("out.txt")
	if err != nil {
		return "", err
	}

	return string(out), nil
}
