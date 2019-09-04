/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package console

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/httputil"
	"devt.de/krotik/common/httputil/access"
	"devt.de/krotik/common/httputil/auth"
	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/api/ac"
	v1 "devt.de/krotik/eliasdb/api/v1"
	"devt.de/krotik/eliasdb/config"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

const TESTPORT = ":9090"

var credGiver *CredGiver

type CredGiver struct {
	UserQueue []string
	PassQueue []string
}

func (cg *CredGiver) Reset() {
	cg.UserQueue = nil
	cg.PassQueue = nil
}

func (cg *CredGiver) GetCredentials() (string, string) {
	if len(cg.PassQueue) > 0 {
		var u, pw string

		u, cg.UserQueue = cg.UserQueue[0], cg.UserQueue[1:]
		pw, cg.PassQueue = cg.PassQueue[0], cg.PassQueue[1:]

		return u, pw
	}

	return "***user***", "***pass***"
}

func ResetDB() {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	api.GM = gm
	api.GS = mgs
}

func TestMain(m *testing.M) {
	var err error

	flag.Parse()

	// Change ReadLine function

	credGiver = &CredGiver{}

	// Initialise config

	config.LoadDefaultConfig()

	// Initialise DB

	ResetDB()

	// Start the server

	hs, wg := startServer()
	if hs == nil {
		return
	}

	// Disable access logging

	ac.LogAccess = func(v ...interface{}) {}

	// Register public endpoints

	api.RegisterRestEndpoints(api.GeneralEndpointMap)
	api.RegisterRestEndpoints(ac.PublicAccessControlEndpointMap)

	// Initialise auth handler

	ac.AuthHandler = auth.NewCookieAuthHandleFuncWrapper(http.HandleFunc)

	// Important statement! - all registered endpoints afterwards
	// are subject to access control

	api.HandleFunc = ac.AuthHandler.HandleFunc

	// Register management endpoints

	api.RegisterRestEndpoints(ac.AccessManagementEndpointMap)
	api.RegisterRestEndpoints(v1.V1EndpointMap)

	// Initialise user DB

	ac.UserDB, err = datautil.NewEnforcedUserDB("test_user.db", "")
	errorutil.AssertOk(err)

	// Put the UserDB in charge of verifying passwords

	ac.AuthHandler.SetAuthFunc(ac.UserDB.CheckUserPassword)

	// Initialise ACL's

	var conf map[string]interface{}

	errorutil.AssertOk(json.Unmarshal(stringutil.StripCStyleComments(ac.DefaultAccessDB), &conf))
	at, err := access.NewMemoryACLTableFromConfig(conf)
	errorutil.AssertOk(err)
	ac.InitACLs(at)

	// Connect the ACL object to the AuthHandler - this provides authorization for users

	ac.AuthHandler.SetAccessFunc(ac.ACL.CheckHTTPRequest)

	// Adding special handlers which redirect to the login page

	ac.AuthHandler.CallbackSessionExpired = ac.CallbackSessionExpired
	ac.AuthHandler.CallbackUnauthorized = ac.CallbackUnauthorized

	// Add users

	ac.UserDB.UserDB.AddUserEntry("elias", "elias", nil)
	ac.UserDB.UserDB.AddUserEntry("johndoe", "doe", nil)

	// Disable debounce time for unit tests

	ac.DebounceTime = 0

	// Run the tests

	res := m.Run()

	// Stop the server

	stopServer(hs, wg)

	// Stop ACL monitoring

	ac.ACL.Close()

	// Remove files

	os.Remove("test_user.db")

	os.Exit(res)
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

func createSongGraph() {

	constructEdge := func(key string, node1 data.Node, node2 data.Node, number int) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "Wrote")

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "Author")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "Song")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr("number", number)

		return edge
	}

	storeSong := func(node data.Node, name string, ranking int, number int) {
		node3 := data.NewGraphNode()
		node3.SetAttr("key", name)
		node3.SetAttr("kind", "Song")
		node3.SetAttr("name", name)
		node3.SetAttr("ranking", ranking)
		api.GM.StoreNode("main", node3)
		api.GM.StoreEdge("main", constructEdge(name, node, node3, number))
	}

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "Author")
	node0.SetAttr("name", "John")
	node0.SetAttr("desc", "A lonely artisT")
	api.GM.StoreNode("main", node0)

	storeSong(node0, "Aria1", 8, 1)
	storeSong(node0, "Aria2", 2, 2)
	storeSong(node0, "Aria3", 4, 3)
	storeSong(node0, "Aria4", 18, 4)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "Author")
	node1.SetAttr("name", "Mike")
	node1.SetAttr("desc", "An annoying artist")
	api.GM.StoreNode("main", node1)

	storeSong(node1, "LoveSong3", 1, 3)
	storeSong(node1, "FightSong4", 3, 4)
	storeSong(node1, "DeadSong2", 6, 2)
	storeSong(node1, "StrangeSong1", 5, 1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "Writer")
	node2.SetAttr("name", "Hans")
	node2.SetAttr("text", "A song writer for an artist")
	api.GM.StoreNode("main", node2)

	storeSong(node2, "MyOnlySong3", 19, 3)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "123")
	node3.SetAttr("kind", "Producer")
	node3.SetAttr("name", "Jack")
	node3.SetAttr("occupation", "A producer of an aRtIsT")
	api.GM.StoreNode("second", node3)

	// Create lots of spam nodes

	for i := 0; i < 21; i++ {
		nodespam := data.NewGraphNode()
		nodespam.SetAttr("key", "000"+strconv.Itoa(i))
		nodespam.SetAttr("kind", "Spam")
		nodespam.SetAttr("name", "Spam"+strconv.Itoa(i))
		api.GM.StoreNode("main", nodespam)
	}
}

func TestDescriptions(t *testing.T) {
	var out bytes.Buffer

	ResetDB()

	// Enable access control

	config.Config[config.EnableAccessControl] = true
	defer func() {
		config.Config[config.EnableAccessControl] = false
	}()

	c := NewConsole("http://localhost"+TESTPORT, &out, credGiver.GetCredentials,
		func() string { return "***pass***" },
		func(args []string, e *bytes.Buffer) error {
			return nil
		})

	for _, cmd := range c.Commands() {
		if ok, err := c.Run("help " + cmd.Name()); !ok || err != nil {
			t.Error(ok, err)
			return
		}
	}

	if res := out.String(); res != `
Exports the data which is currently in the export buffer. The export buffer is filled with the previous command output in a machine readable form.
Do a full-text search of the database.
Grants a new permission to a group. Specify first the permission in CRUD format (Create, Read, Update or Delete), then a resource path and then a group name.
Adds a group to the system.
Removes a group from the system.
Returns a list of all groups and their permissions.
Display descriptions for all available commands.
Returns general database information such as known node kinds, known attributes, etc ...
Joins a user to a group.
Removes a user from a group.
Log in as a user.
Log out the current user.
Changes the password of a user.
Displays or sets the current partition.
Revokes permissions to a resource for a group.
Adds a user to the system.
Removes a user from the system.
Returns a table of all users and their groups.
Displays server version information.
Returns the current login status.
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("help foo"); ok || err == nil || err.Error() != "Unknown command: foo" {
		t.Error(ok, err)
		return
	}
}

func TestNoAuthentication(t *testing.T) {
	var out bytes.Buffer
	var export bytes.Buffer

	ResetDB()
	credGiver.Reset()

	c := NewConsole("http://localhost"+TESTPORT, &out, credGiver.GetCredentials,
		func() string { return "***pass***" },
		func(args []string, e *bytes.Buffer) error {
			export = *e
			return nil
		})

	// Disable authentication

	auth.TestCookieAuthDisabled = true
	defer func() {
		auth.TestCookieAuthDisabled = false
	}()

	// Check we don't have access control commands

	if ok, err := c.Run("whoami"); ok || err == nil || err.Error() != "Unknown command" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("ver"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
EliasDB `[1:]+config.ProductVersion+` (REST versions: [v1])
` {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("info"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌─────┬──────┐
│Kind │Count │
└─────┴──────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	createSongGraph()

	if ok, err := c.Run("info"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌─────────┬───────────┐
│Kind     │Count      │
├─────────┼───────────┤
│Author   │         2 │
│Producer │         1 │
│Song     │         9 │
│Spam     │        21 │
│Writer   │         1 │
└─────────┴───────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("export"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := export.String(); res != `
Kind, Count
Author, 2
Producer, 1
Song, 9
Spam, 21
Writer, 1
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	if ok, err := c.Run("help"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Command Description
export  Exports the last output.
find    Do a full-text search of the database.
help    Display descriptions for all available commands.
info    Returns general database information.
part    Displays or sets the current partition.
ver     Displays server version information.
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestBasicCommands(t *testing.T) {
	var out bytes.Buffer

	ResetDB()
	credGiver.Reset()

	// Enable access control

	config.Config[config.EnableAccessControl] = true
	defer func() {
		config.Config[config.EnableAccessControl] = false
	}()

	c := NewConsole("http://localhost"+TESTPORT, &out, credGiver.GetCredentials,
		func() string { return "***pass***" },
		func(args []string, e *bytes.Buffer) error {
			return nil
		})

	// Special command - this should not require a login and should return nobody

	if ok, err := c.Run("whoami"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if strings.TrimSpace(out.String()) != "Nobody - not logged in" {
		t.Error("Unexpected result:", out.String())
		return
	}

	// Now force the login - we should get one failed login

	out.Reset()

	credGiver.UserQueue = []string{"elias", "elias"}
	credGiver.PassQueue = []string{"elia", "elias"}

	if ok, err := c.Run("users"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Login failed for user elias: Unauthorized (error=<nil>)
Login as user elias
┌─────────┬─────────────┐
│Username │Groups       │
├─────────┼─────────────┤
│elias    │admin/public │
│johndoe  │public       │
└─────────┴─────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("help"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Command    Description
export     Exports the last output.
find       Do a full-text search of the database.
grantperm  Grants a new permission to a group.
groupadd   Adds a group to the system.
groupdel   Removes a group from the system.
groups     Returns a list of all groups and their permissions.
help       Display descriptions for all available commands.
info       Returns general database information.
joingroup  Joins a user to a group.
leavegroup Removes a user from a group.
login      Log in as a user.
logout     Log out the current user.
newpass    Changes the password of a user.
part       Displays or sets the current partition.
revokeperm Revokes permissions to a resource for a group.
useradd    Adds a user to the system.
userdel    Removes a user from the system.
users      Returns a list of all users.
ver        Displays server version information.
whoami     Returns the current login status.
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	// Test log out

	out.Reset()

	if ok, err := c.Run("logout"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `Current user logged out.
` {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("whoami"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `Nobody - not logged in
` {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	credGiver.UserQueue = []string{"elias"}
	credGiver.PassQueue = []string{"elias"}

	if ok, err := c.Run("login"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `Login as user elias
` {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("users"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌─────────┬─────────────┐
│Username │Groups       │
├─────────┼─────────────┤
│elias    │admin/public │
│johndoe  │public       │
└─────────┴─────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("whoami"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
elias
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("logout"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `Current user logged out.
` {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	credGiver.UserQueue = []string{""}
	credGiver.PassQueue = []string{""}

	if ok, err := c.Run("login"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `Skipping authentication
` {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("users"); ok || err == nil || err.Error() != "GET request to /db/user/u/ failed: Valid credentials required" {
		t.Error("Unexpected result:", ok, err)
		return
	}

	if res := out.String(); res != "" {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()
}
