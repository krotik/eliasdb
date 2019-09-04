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
Package server contains the code for the EliasDB server.
*/
package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"devt.de/krotik/common/cryptutil"
	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/common/httputil"
	"devt.de/krotik/common/httputil/access"
	"devt.de/krotik/common/httputil/auth"
	"devt.de/krotik/common/httputil/user"
	"devt.de/krotik/common/lockutil"
	"devt.de/krotik/common/timeutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/api/ac"
	"devt.de/krotik/eliasdb/api/v1"
	"devt.de/krotik/eliasdb/cluster"
	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/config"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

/*
Using custom consolelogger type so we can test log.Fatal calls with unit tests. Overwrite
these if the server should not call os.Exit on a fatal error.
*/
type consolelogger func(v ...interface{})

var fatal = consolelogger(log.Fatal)
var print = consolelogger(log.Print)

/*
Base path for all file (used by unit tests)
*/
var basepath = ""

/*
UserDBPassphrase is the passphrase which will be used for the user db (only used if
access control is enabled)
*/
var UserDBPassphrase = ""

/*
StartServer runs the EliasDB server. The server uses config.Config for all its configuration
parameters.
*/
func StartServer() {
	StartServerWithSingleOp(nil)
}

/*
StartServerWithSingleOp runs the EliasDB server. If the singleOperation function is
not nil then the server executes the function and exists if the function returns true.
*/
func StartServerWithSingleOp(singleOperation func(*graph.Manager) bool) {
	var err error
	var gs graphstorage.Storage

	print(fmt.Sprintf("EliasDB %v", config.ProductVersion))

	// Ensure we have a configuration - use the default configuration if nothing was set

	if config.Config == nil {
		config.LoadDefaultConfig()
	}

	// Create graph storage

	if config.Bool(config.MemoryOnlyStorage) {

		print("Starting memory only datastore")

		gs = graphstorage.NewMemoryGraphStorage(config.MemoryOnlyStorage)

		if config.Bool(config.EnableReadOnly) {
			print("Ignoring EnableReadOnly setting")
		}

	} else {

		loc := filepath.Join(basepath, config.Str(config.LocationDatastore))
		readonly := config.Bool(config.EnableReadOnly)

		if readonly {
			print("Starting datastore (readonly) in ", loc)
		} else {
			print("Starting datastore in ", loc)
		}

		// Ensure path for database exists

		ensurePath(loc)

		gs, err = graphstorage.NewDiskGraphStorage(loc, readonly)
		if err != nil {
			fatal(err)
			return
		}
	}

	// Check if clustering is enabled

	if config.Bool(config.EnableCluster) {

		print("Reading cluster config")

		cconfig, err := fileutil.LoadConfig(filepath.Join(basepath, config.Str(config.ClusterConfigFile)),
			manager.DefaultConfig)

		if err != nil {
			fatal("Failed to load cluster config:", err)
			return
		}

		print("Opening cluster state info")

		si, err := manager.NewDefaultStateInfo(filepath.Join(basepath, config.Str(config.ClusterStateInfoFile)))
		if err != nil {
			fatal("Failed to load cluster state info:", err)
			return
		}

		loghist := int(config.Int(config.ClusterLogHistory))

		print(fmt.Sprintf("Starting cluster (log history: %v)", loghist))

		ds, err := cluster.NewDistributedStorage(gs, cconfig, si)
		if err != nil {
			fatal("Failed to create distributed storage:", err)
			return
		}

		gs = ds

		// Make the distributed storage and the cluster log available for the REST API

		api.DD = ds
		api.DDLog = datautil.NewRingBuffer(loghist)

		logFunc := func(v ...interface{}) {
			api.DDLog.Log(timeutil.MakeTimestamp(), " ", fmt.Sprint(v...))
		}
		logPrintFunc := func(v ...interface{}) {
			print("[Cluster] ", fmt.Sprint(v...))
			api.DDLog.Log(timeutil.MakeTimestamp(), " ", fmt.Sprint(v...))
		}

		manager.LogDebug = logFunc
		manager.LogInfo = logPrintFunc

		// Kick off the cluster

		ds.MemberManager.Start()
	}

	// Create GraphManager

	print("Creating GraphManager instance")

	api.GS = gs
	api.GM = graph.NewGraphManager(gs)

	defer func() {

		print("Closing datastore")

		if err := gs.Close(); err != nil {
			fatal(err)
			return
		}

		os.RemoveAll(filepath.Join(basepath, config.Str(config.LockFile)))
	}()

	// Handle single operation - these are operations which work on the GraphManager
	// and then exit.

	if singleOperation != nil && singleOperation(api.GM) {
		return
	}

	// Setting other API parameters

	// Setup cookie expiry

	cookieMaxAge := int(config.Int(config.CookieMaxAgeSeconds))
	auth.CookieMaxLifetime = cookieMaxAge
	user.CookieMaxLifetime = cookieMaxAge
	user.UserSessionManager.Provider.(*user.MemorySessionProvider).SetExpiry(cookieMaxAge)

	api.APIHost = config.Str(config.HTTPSHost) + ":" + config.Str(config.HTTPSPort)
	v1.ResultCacheMaxSize = uint64(config.Int(config.ResultCacheMaxSize))
	v1.ResultCacheMaxAge = config.Int(config.ResultCacheMaxAgeSeconds)

	// Check if HTTPS key and certificate are in place

	keyPath := filepath.Join(basepath, config.Str(config.LocationHTTPS), config.Str(config.HTTPSKey))
	certPath := filepath.Join(basepath, config.Str(config.LocationHTTPS), config.Str(config.HTTPSCertificate))

	keyExists, _ := fileutil.PathExists(keyPath)
	certExists, _ := fileutil.PathExists(certPath)

	if !keyExists || !certExists {

		// Ensure path for ssl files exists

		ensurePath(filepath.Join(basepath, config.Str(config.LocationHTTPS)))

		print("Creating key (", config.Str(config.HTTPSKey), ") and certificate (",
			config.Str(config.HTTPSCertificate), ") in: ", config.Str(config.LocationHTTPS))

		// Generate a certificate and private key

		err = cryptutil.GenCert(filepath.Join(basepath, config.Str(config.LocationHTTPS)),
			config.Str(config.HTTPSCertificate), config.Str(config.HTTPSKey),
			"localhost", "", 365*24*time.Hour, false, 4096, "")

		if err != nil {
			fatal("Failed to generate ssl key and certificate:", err)
			return
		}
	}

	// Register public REST endpoints - these will never be checked for authentication

	api.RegisterRestEndpoints(api.GeneralEndpointMap)

	// Setup access control

	if config.Bool(config.EnableAccessControl) {

		// Register REST endpoints for access control

		api.RegisterRestEndpoints(ac.PublicAccessControlEndpointMap)

		// Setup user database

		ac.UserDB, err = datautil.NewEnforcedUserDB(filepath.Join(basepath, config.Str(config.LocationUserDB)),
			UserDBPassphrase)

		if err == nil {
			var ok bool

			// Setup access control - this will initialise the global ACL (access
			// control lists) object

			if ok, err = fileutil.PathExists(filepath.Join(basepath, config.Str(config.LocationAccessDB))); !ok && err == nil {
				err = ioutil.WriteFile(filepath.Join(basepath, config.Str(config.LocationAccessDB)), ac.DefaultAccessDB, 0600)
			}

			if err == nil {
				tab, err := access.NewPersistedACLTable(filepath.Join(basepath, config.Str(config.LocationAccessDB)), 3*time.Second)

				if err == nil {
					ac.InitACLs(tab)
				}
			}
		}

		if err == nil {

			// Make sure there are the initial accounts (circumventing the
			// enforced password constrains by using the embedded UserDB directly)

			if len(ac.UserDB.AllUsers()) == 0 {
				ac.UserDB.UserDB.AddUserEntry("elias", "elias", nil)
				ac.UserDB.UserDB.AddUserEntry("johndoe", "doe", nil)
			}

			// Setup the AuthHandler object which provides cookie based authentication
			// for endpoints which are registered with its HandleFunc

			ac.AuthHandler = auth.NewCookieAuthHandleFuncWrapper(http.HandleFunc)

			// Connect the UserDB object to the AuthHandler - this provides authentication for users

			ac.AuthHandler.SetAuthFunc(ac.UserDB.CheckUserPassword)

			// Connect the ACL object to the AuthHandler - this provides authorization for users

			ac.AuthHandler.SetAccessFunc(ac.ACL.CheckHTTPRequest)

			// Make login page a "public" page i.e. a page which can be reached without
			// authentication

			ac.AuthHandler.AddPublicPage("/login.html",
				httputil.SingleFileServer(filepath.Join(
					config.Str(config.LocationWebFolder), "login.html"),
					nil).ServeHTTP)

			// Also make the fingerprint.json a public page

			ac.AuthHandler.AddPublicPage("/fingerprint.json",
				httputil.SingleFileServer(filepath.Join(
					config.Str(config.LocationWebFolder), "fingerprint.json"),
					nil).ServeHTTP)

			// Adding special handlers which redirect to the login page

			ac.AuthHandler.CallbackSessionExpired = ac.CallbackSessionExpired
			ac.AuthHandler.CallbackUnauthorized = ac.CallbackUnauthorized

			// Finally set the HandleFunc of the AuthHandler as the HandleFunc of the API

			api.HandleFunc = ac.AuthHandler.HandleFunc

			// After the api.HandleFunc has been set we can now register the management
			// endpoints which should be subject to access control

			api.RegisterRestEndpoints(ac.AccessManagementEndpointMap)
		}
	}

	// Register EliasDB API endpoints - depending on if access control has been enabled
	// these will require authentication and authorization for a given user

	api.RegisterRestEndpoints(v1.V1EndpointMap)

	// Register normal web server

	if config.Bool(config.EnableWebFolder) {
		webFolder := filepath.Join(basepath, config.Str(config.LocationWebFolder))

		print("Ensuring web folder: ", webFolder)

		ensurePath(webFolder)

		fs := http.FileServer(http.Dir(webFolder))

		api.HandleFunc("/", fs.ServeHTTP)

		// Write login

		if config.Bool(config.EnableAccessControl) {

			loginFile := filepath.Join(webFolder, "login.html")

			print("Ensuring login page: ", loginFile)

			if res, _ := fileutil.PathExists(loginFile); !res {
				errorutil.AssertOk(ioutil.WriteFile(loginFile, []byte(LoginSRC[1:]), 0644))
			}
		}

		// Write terminal(s)

		if config.Bool(config.EnableWebTerminal) {

			ensurePath(filepath.Join(webFolder, api.APIRoot))

			termFile := filepath.Join(webFolder, api.APIRoot, "term.html")

			print("Ensuring web terminal: ", termFile)

			if res, _ := fileutil.PathExists(termFile); !res {
				errorutil.AssertOk(ioutil.WriteFile(termFile, []byte(TermSRC[1:]), 0644))
			}
		}

		if config.Bool(config.EnableClusterTerminal) {

			ensurePath(filepath.Join(webFolder, api.APIRoot))

			termFile := filepath.Join(webFolder, api.APIRoot, "cluster.html")

			if config.Bool(config.EnableCluster) {

				// Add the url to the member info of the member manager

				api.DD.MemberManager.MemberInfo()[manager.MemberInfoTermURL] =
					fmt.Sprintf("https://%v:%v%v/%v", config.Str(config.HTTPSHost),
						config.Str(config.HTTPSPort), api.APIRoot, "cluster.html")
			}

			print("Ensuring cluster terminal: ", termFile)

			if res, _ := fileutil.PathExists(termFile); !res {
				errorutil.AssertOk(ioutil.WriteFile(termFile, []byte(ClusterTermSRC[1:]), 0644))
			}
		}
	}

	// Start HTTPS server and enable REST API

	hs := &httputil.HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	port := config.Str(config.HTTPSPort)

	print("Starting server on: ", api.APIHost)

	go hs.RunHTTPSServer(basepath+config.Str(config.LocationHTTPS), config.Str(config.HTTPSCertificate),
		config.Str(config.HTTPSKey), ":"+port, &wg)

	// Wait until the server has started

	wg.Wait()

	// HTTPS Server has started

	if hs.LastError != nil {
		fatal(hs.LastError)
		return
	}

	// Read server certificate and write a fingerprint file

	fpfile := filepath.Join(basepath, config.Str(config.LocationWebFolder), "fingerprint.json")

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

	lf := lockutil.NewLockFile(basepath+config.Str(config.LockFile), time.Duration(2)*time.Second)

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

	if config.Bool(config.EnableCluster) {

		// Shutdown cluster

		gs.(*cluster.DistributedStorage).MemberManager.Shutdown()
	}
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
