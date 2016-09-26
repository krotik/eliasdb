/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
EliasDB is a graph based database which aims to provide a lightweight solution
for projects which want to store their data as a graph.

Features

- Build on top of a fast key-value store which supports transactions and memory-only storage.

- Data is stored in nodes (key-value objects) which are connected via edges.

- Stored graphs can be separated via partitions.

- Stored graphs support cascading deletions - delete one node and all its "children".

- All stored data is indexed and can be quickly searched via a full text phrase search.

- For more complex queries EliasDB has an own query language called EQL with an sql-like syntax.

- Written in Go from scratch. No third party libraries were used apart from Go's standard library.

- The database can be embedded or used as a standalone application.

- When used as a standalone application it comes with an internal HTTPS webserver which provides a REST API and a basic file server.

- When used as an embedded database it supports transactions with rollbacks, iteration of data and rule based consistency management.
*/
package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"devt.de/common/cryptutil"
	"devt.de/common/fileutil"
	"devt.de/common/httputil"
	"devt.de/common/lockutil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/api/v1"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/version"
)

// Global variables
// ================

/*
ConfigFile is the config file which will be used to configure EliasDB
*/
var ConfigFile = "eliasdb.config.json"

/*
Known configuration options for EliasDB
*/
const (
	MemoryOnlyStorage        = "MemoryOnlyStorage"
	LocationDatastore        = "LocationDatastore"
	LocationHTTPS            = "LocationHTTPS"
	LocationWebFolder        = "LocationWebFolder"
	HTTPSCertificate         = "HTTPSCertificate"
	HTTPSKey                 = "HTTPSKey"
	LockFile                 = "LockFile"
	HTTPSHost                = "HTTPSHost"
	HTTPSPort                = "HTTPSPort"
	EnableReadOnly           = "EnableReadOnly"
	EnableWebFolder          = "EnableWebFolder"
	EnableWebTerminal        = "EnableWebTerminal"
	ResultCacheMaxSize       = "ResultCacheMaxSize"
	ResultCacheMaxAgeSeconds = "ResultCacheMaxAgeSeconds"
)

/*
DefaultConfig is the defaut configuration
*/
var DefaultConfig = map[string]interface{}{
	MemoryOnlyStorage:        false,
	EnableReadOnly:           false,
	EnableWebFolder:          true,
	EnableWebTerminal:        true,
	LocationDatastore:        "db",
	LocationHTTPS:            "ssl",
	LocationWebFolder:        "web",
	HTTPSHost:                "localhost",
	HTTPSPort:                "9090",
	HTTPSCertificate:         "cert.pem",
	HTTPSKey:                 "key.pem",
	LockFile:                 "eliasdb.lck",
	ResultCacheMaxSize:       "",
	ResultCacheMaxAgeSeconds: "",
}

/*
Config is the actual configuration data which is used
*/
var Config map[string]interface{}

// EliasDB Main
// ============

/*
Using custom consolelogger type so we can test log.Fatal calls
with unit tests.
*/
type consolelogger func(v ...interface{})

var fatal consolelogger = log.Fatal
var print consolelogger = log.Print

/*
Base path for all file (used by unit tests)
*/
var basepath = ""

/*
Main entry point for EliasDB.
*/
func main() {
	var err error
	var gs graphstorage.Storage

	print(fmt.Sprintf("EliasDB %v.%v", version.VERSION, version.REV))

	// Load configuration

	if Config == nil {
		Config, err = fileutil.LoadConfig(basepath+ConfigFile, DefaultConfig)
		if err != nil {
			fatal(err)
			return
		}
	}

	if Config[MemoryOnlyStorage].(bool) {

		print("Starting memory only datastore")

		gs = graphstorage.NewMemoryGraphStorage(MemoryOnlyStorage)

		if Config[EnableReadOnly].(bool) {
			print("Ignoring EnableReadOnly setting")
		}

	} else {

		loc := basepath + fmt.Sprint(Config[LocationDatastore])

		if Config[EnableReadOnly].(bool) {
			print("Starting datastore (readonly) in ", loc)
		} else {
			print("Starting datastore in ", loc)
		}

		// Ensure path for database exists

		ensurePath(loc)

		gs, err = graphstorage.NewDiskGraphStorage(loc, Config[EnableReadOnly].(bool))
		if err != nil {
			fatal(err)
			return
		}
	}

	// Create GraphManager

	print("Creating GraphManager instance")

	api.GM = graph.NewGraphManager(gs)
	defer func() {

		print("Closing datastore")

		if err := gs.Close(); err != nil {
			fatal(err)
			return
		}

		os.RemoveAll(basepath + config(LockFile))
	}()

	// Handle command line

	if !handleCommandLine(api.GM) {
		return
	}

	// Setting other API parameters

	api.APIHost = config(HTTPSHost) + ":" + config(HTTPSPort)
	v1.ResultCacheMaxSize, _ = strconv.ParseUint(config(ResultCacheMaxSize), 10, 0)
	v1.ResultCacheMaxAge, _ = strconv.ParseInt(config(ResultCacheMaxAgeSeconds), 10, 0)

	// Check if HTTPS key and certificate are in place

	keyPath := path.Join(basepath, config(LocationHTTPS), config(HTTPSKey))
	certPath := path.Join(basepath, config(LocationHTTPS), config(HTTPSCertificate))

	keyExists, _ := fileutil.PathExists(keyPath)
	certExists, _ := fileutil.PathExists(certPath)

	if !keyExists || !certExists {

		// Ensure path for ssl files exists

		ensurePath(basepath + config(LocationHTTPS))

		print("Creating key (", config(HTTPSKey), ") and certificate (",
			config(HTTPSCertificate), ") in: ", config(LocationHTTPS))

		// Generate a certificate and private key

		err = cryptutil.GenCert(basepath+config(LocationHTTPS), config(HTTPSCertificate),
			config(HTTPSKey), "localhost", "", 365*24*time.Hour, true, 2048, "")

		if err != nil {
			fatal("Failed to generate ssl key and certificate:", err)
			return
		}
	}

	// Register REST endpoints for version 1

	api.RegisterRestEndpoints(v1.V1EndpointMap)
	api.RegisterRestEndpoints(api.GeneralEndpointMap)

	// Register normal web server

	if Config[EnableWebFolder].(bool) {
		webFolder := basepath + config(LocationWebFolder)

		print("Ensuring web folder: ", webFolder)

		ensurePath(webFolder)

		fs := http.FileServer(http.Dir(webFolder))

		api.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fs.ServeHTTP(w, r)
		})

		// Write terminal

		if Config[EnableWebTerminal].(bool) {

			ensurePath(path.Join(webFolder, api.APIRoot))

			termFile := path.Join(webFolder, api.APIRoot, "term.html")

			print("Ensuring web termminal: ", termFile)

			ioutil.WriteFile(termFile, []byte(TermSRC[1:]), 0644)
		}
	}

	// Start HTTPS server and enable REST API

	hs := &httputil.HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	port := config(HTTPSPort)

	print("Starting server on: ", api.APIHost)

	go hs.RunHTTPSServer(basepath+config(LocationHTTPS), config(HTTPSCertificate),
		config(HTTPSKey), ":"+port, &wg)

	// Wait until the server has started

	wg.Wait()

	// HTTPS Server has started

	if hs.LastError != nil {
		fatal(hs.LastError)
		return
	}

	// Read server certificate and write a fingerprint file

	fpfile := basepath + config(LocationWebFolder) + "/fingerprint.json"

	print("Writing fingerprint file: ", fpfile)

	certs, _ := cryptutil.ReadX509CertsFromFile(certPath)

	if len(certs) > 0 {
		buf := bytes.Buffer{}

		buf.WriteString("{\n")
		buf.WriteString(fmt.Sprintf(`  "md5"    : "%s",`, cryptutil.Md5CertFingerprint(certs[0])))
		buf.WriteString("\n")
		buf.WriteString(fmt.Sprintf(`  "sha1"   : "%s",`, cryptutil.Sha1CertFingerprint(certs[0])))
		buf.WriteString("\n")
		buf.WriteString(fmt.Sprintf(`  "sha256" : "%s"`, cryptutil.Sha256CertFingerprint(certs[0])))
		buf.WriteString("\n")
		buf.WriteString("}\n")

		ioutil.WriteFile(fpfile, buf.Bytes(), 0644)
	}

	// Create a lockfile so the server can be shut down

	lf := lockutil.NewLockFile(basepath+config(LockFile), time.Duration(2)*time.Second)

	lf.Start()

	go func() {

		// Check if the lockfile watcher is running and
		// call shutdown once it has finished

		for lf.WatcherRunning() {
			time.Sleep(time.Duration(1) * time.Second)
		}

		print("Lockfile was modified")

		hs.Shutdown()
	}()

	// Add to the wait group so we can wait for the shutdown

	wg.Add(1)

	print("Waiting for shutdown")
	wg.Wait()

	print("Shutting down")
}
