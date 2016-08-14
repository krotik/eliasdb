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
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"devt.de/common/fileutil"
	"devt.de/common/httputil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/graph/graphstorage"
)

const TESTDB = "testdb"

const INVALID_FILE_NAME = "**" + string(0x0)

var printLog = []string{}
var errorLog = []string{}

var printLogging = false

func TestMain(m *testing.M) {
	flag.Parse()

	basepath = TESTDB + "/"

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

	if res, _ := fileutil.PathExists(TESTDB); res {
		if err := os.RemoveAll(TESTDB); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
	}

	ensurePath(TESTDB)

	// Run the tests

	res := m.Run()

	if res, _ := fileutil.PathExists(TESTDB); res {
		if err := os.RemoveAll(TESTDB); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
	}

	os.Exit(res)
}

func TestMainNormalCase(t *testing.T) {

	// Make sure to reset the DefaultServeMux

	defer func() { http.DefaultServeMux = http.NewServeMux() }()

	// Make sure to remove any files

	defer func() {
		if err := os.RemoveAll(TESTDB); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
		ensurePath(TESTDB)
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

	// Kick off main function

	go func() {
		main()
		errorChan <- nil
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
Starting datastore in testdb/db
Creating GraphManager instance
Creating key (key.pem) and certificate (cert.pem) in: ssl
Ensuring web folder: testdb/web
Ensuring web termminal: testdb/web/db/term.html
Starting server on: localhost:9090
Writing fingerprint file: testdb/web/fingerprint.json
Waiting for shutdown
Lockfile was modified
Shutting down
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
		if err := os.RemoveAll(TESTDB); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
		ensurePath(TESTDB)
	}()

	// Setup config and logs

	Config = nil
	origConfFile := ConfigFile
	origBasePath := basepath

	basepath = ""
	ConfigFile = INVALID_FILE_NAME

	printLog = []string{}
	errorLog = []string{}

	main()

	// Check that an error happend

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

	Config[LocationDatastore] = INVALID_FILE_NAME

	main()

	// Check that an error happend

	if len(errorLog) != 2 ||
		!strings.Contains(errorLog[0], "Could not create directory") ||
		!strings.Contains(errorLog[1], "Failed to open graph storage") {
		t.Error("Unexpected error:", errorLog)
		return
	}

	// Set back logs

	printLog = []string{}
	errorLog = []string{}

	// Use memory only storage

	Config[MemoryOnlyStorage] = true

	// Test failed ssl key generation

	Config[HTTPSKey] = INVALID_FILE_NAME

	main()

	// Check that an error happend

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

	main()

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
