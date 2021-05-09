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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/cluster"
	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestClusterStorage(t *testing.T) {

	clusterQueryURL := "http://localhost" + TESTPORT + EndpointClusterQuery
	graphURL := "http://localhost" + TESTPORT + EndpointGraph

	cluster2 := createCluster(2)

	joinCluster(cluster2, t)

	oldGM := api.GM
	oldGS := api.GS
	api.GS = cluster2[0]
	api.GM = graph.NewGraphManager(cluster2[0])
	api.DD = cluster2[0]
	api.DDLog = datautil.NewRingBuffer(10)

	defer func() {
		api.GM = oldGM
		api.GS = oldGS
		api.DD = nil
		api.DDLog = nil
	}()

	// We should now get back a state

	st, _, res := sendTestRequest(clusterQueryURL, "GET", nil)

	if st != "200 OK" || res != `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "2"
  ],
  "tsold": [
    "TestClusterMember-0",
    "1"
  ]
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Insert some data

	sendTestRequest(graphURL+"i41health/n", "POST", []byte(`
[{
	"key":"3",
	"kind":"Upload",
	"parcel": "12345"
}]
`[1:]))

	cluster.WaitForTransfer()

	n, err := api.GM.FetchNode("i41health", "3", "Upload")

	if err != nil || n.String() != `GraphNode:
       key : 3
      kind : Upload
    parcel : 12345
` {
		t.Error("Unexpected result:", n, err)
		return
	}
}

func TestClusterQuery(t *testing.T) {

	queryURL := "http://localhost" + TESTPORT + EndpointClusterQuery

	st, _, res := sendTestRequest(queryURL, "GET", nil)

	// We should get a failure back if clustering is not available

	if st != "503 Service Unavailable" || res != "Clustering is not enabled on this instance" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL, "DELETE", nil)

	// We should get a failure back if clustering is not available

	if st != "503 Service Unavailable" || res != "Clustering is not enabled on this instance" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Create now a small cluster

	cluster2 := createCluster(2)

	oldGM := api.GM
	oldGS := api.GS
	api.GS = cluster2[0]
	api.GM = graph.NewGraphManager(cluster2[0])
	api.DD = cluster2[0]
	api.DDLog = datautil.NewRingBuffer(10)

	defer func() {
		api.GM = oldGM
		api.GS = oldGS
		api.DD = nil
		api.DDLog = nil
	}()

	// We should now get back a state

	st, _, res = sendTestRequest(queryURL, "GET", nil)

	if st != "200 OK" || res != `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "1"
  ],
  "tsold": [
    "",
    "0"
  ]
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"memberinfos", "GET", nil)

	if st != "200 OK" || res != `
{
  "TestClusterMember-0": {}
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	api.DDLog.Add("test cluster message1")
	api.DDLog.Add("test cluster message2")

	st, _, res = sendTestRequest(queryURL+"log", "GET", nil)

	if st != "200 OK" || res != `
[
  "test cluster message1",
  "test cluster message2"
]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, _ = sendTestRequest(queryURL+"log", "DELETE", nil)

	if st != "200 OK" {
		t.Error("Unexpected response:", st)
		return
	}

	st, _, res = sendTestRequest(queryURL+"log", "GET", nil)

	if st != "200 OK" || res != `
[]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, _ = sendTestRequest(queryURL+"bla", "DELETE", nil)

	if st != "400 Bad Request" {
		t.Error("Unexpected response:", st)
		return
	}

	log.SetOutput(ioutil.Discard)
	cluster2[0].MemberManager.Start()
	cluster2[1].MemberManager.Start()
	defer func() {
		cluster2[0].MemberManager.Shutdown()
		cluster2[1].MemberManager.Shutdown()
		log.SetOutput(os.Stdout)
	}()

	jsonString, err := json.Marshal(map[string]interface{}{
		"name":    cluster2[1].MemberManager.Name(),
		"netaddr": cluster2[1].MemberManager.NetAddr(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"ping", "PUT", jsonString)

	if st != "200 OK" || res != `[
  "Pong"
]` {
		t.Error("Unexpected response:", st, res)
		return
	}

	manager.MemberErrors = make(map[string]error)
	manager.MemberErrors[cluster2[1].Name()] = errors.New("testerror")

	sendTestRequest(queryURL+"eject", "PUT", jsonString)

	st, _, res = sendTestRequest(queryURL+"ping", "PUT", jsonString)

	if st != "403 Forbidden" || res != "Ping returned an error: ClusterError: Member error (testerror)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	manager.MemberErrors = nil

	st, _, res = sendTestRequest(queryURL, "PUT", nil)

	if st != "400 Bad Request" || res != "Need a command either: join or eject" {
		t.Error("Unexpected response:", st, res)
		return
	}

	jsonString, err = json.Marshal(map[string]interface{}{
		"name": "bla",
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"join", "PUT", jsonString)

	if st != "400 Bad Request" || res != "Required argument netaddr missing in body arguments" {
		t.Error("Unexpected response:", st, res)
		return
	}

	jsonString, err = json.Marshal(map[string]interface{}{
		"name":    "bla",
		"netaddr": cluster2[1].MemberManager.NetAddr(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"join", "PUT", jsonString)

	if st != "403 Forbidden" || res != "Could not join the cluster: ClusterError: Member error (Unknown target member)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	jsonString, err = json.Marshal(map[string]interface{}{
		"name":    cluster2[1].MemberManager.Name(),
		"netaddr": cluster2[1].MemberManager.NetAddr(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"join", "PUT", jsonString)

	if st != "200 OK" || res != "" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check state info

	if err := checkStateInfo(cluster2[1].MemberManager, `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-1",
    "2"
  ],
  "tsold": [
    "TestClusterMember-1",
    "1"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Set some member info and read it back

	cluster2[1].MemberManager.MemberInfo()["test123"] = "123"

	st, _, res = sendTestRequest(queryURL+"memberinfos", "GET", nil)

	if st != "200 OK" || res != `
{
  "TestClusterMember-0": {},
  "TestClusterMember-1": {
    "test123": "123"
  }
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Eject member from cluster

	jsonString, err = json.Marshal(map[string]interface{}{
		"name": cluster2[0].MemberManager.Name(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	manager.MemberErrors = make(map[string]error)
	manager.MemberErrors[cluster2[1].Name()] = errors.New("testerror")

	st, _, res = sendTestRequest(queryURL+"eject", "PUT", jsonString)

	if st != "403 Forbidden" || res != "Could not eject TestClusterMember-0 from cluster: ClusterError: Member error (testerror)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	manager.MemberErrors = nil

	jsonString, err = json.Marshal(map[string]interface{}{
		"name": cluster2[0].MemberManager.Name(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"eject", "PUT", jsonString)

	if st != "200 OK" || res != "" {
		t.Error("Unexpected response:", st, res)
		return
	}

	if err := checkStateInfo(cluster2[1].MemberManager, `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-1",
    "3"
  ],
  "tsold": [
    "TestClusterMember-1",
    "2"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Some error cases

	st, _, res = sendTestRequest(queryURL+"bla", "PUT", jsonString[2:])
	if st != "400 Bad Request" || !strings.HasPrefix(res, "Could not decode arguments") {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"bla", "PUT", jsonString)
	if st != "400 Bad Request" || res != "Unknown command: bla" {
		t.Error("Unexpected response:", st, res)
		return
	}
}

func TestClusterQueryBigCluster(t *testing.T) {

	queryURL := "http://localhost" + TESTPORT + EndpointClusterQuery

	// Create a big cluster

	cluster3 := createCluster(3)

	for _, dd := range cluster3 {
		dd.Start()
		defer dd.Close()
	}

	oldGM := api.GM
	oldGS := api.GS
	api.GS = cluster3[0]
	api.GM = graph.NewGraphManager(cluster3[0])
	api.DD = cluster3[0]
	api.DDLog = datautil.NewRingBuffer(10)

	defer func() {
		api.GM = oldGM
		api.GS = oldGS
		api.DD = nil
		api.DDLog = nil
	}()

	// We should now get back a state

	st, _, res := sendTestRequest(queryURL, "GET", nil)

	if st != "200 OK" || res != `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "1"
  ],
  "tsold": [
    "",
    "0"
  ]
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Join more cluster members

	api.DD = cluster3[1]
	api.DDLog = datautil.NewRingBuffer(10)

	jsonString, err := json.Marshal(map[string]interface{}{
		"name":    cluster3[0].MemberManager.Name(),
		"netaddr": cluster3[0].MemberManager.NetAddr(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"join", "PUT", jsonString)

	if st != "200 OK" || res != "" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL, "GET", nil)

	if st != "200 OK" || res != `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "2"
  ],
  "tsold": [
    "TestClusterMember-0",
    "1"
  ]
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	api.DD = cluster3[2]
	api.DDLog = datautil.NewRingBuffer(10)

	jsonString, err = json.Marshal(map[string]interface{}{
		"name":    cluster3[0].MemberManager.Name(),
		"netaddr": cluster3[0].MemberManager.NetAddr(),
	})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"join", "PUT", jsonString)

	if st != "200 OK" || res != "" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL, "GET", nil)

	if st != "200 OK" || res != `
{
  "failed": null,
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "3"
  ],
  "tsold": [
    "TestClusterMember-0",
    "2"
  ]
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}
}

/*
Create a cluster with n members (all storage is in memory)
*/
func createCluster(n int) []*cluster.DistributedStorage {

	// By default no log output

	log.SetOutput(ioutil.Discard)

	var mgs []*graphstorage.MemoryGraphStorage
	var cs []*cluster.DistributedStorage

	cluster.ClearMSMap()

	for i := 0; i < n; i++ {
		mgs = append(mgs, graphstorage.NewMemoryGraphStorage(fmt.Sprintf("mgs%v", i+1)).(*graphstorage.MemoryGraphStorage))
	}

	for i := 0; i < n; i++ {
		ds, _ := cluster.NewDistributedStorage(mgs[i], map[string]interface{}{
			manager.ConfigRPC:           fmt.Sprintf("localhost:%v", 9020+i),
			manager.ConfigMemberName:    fmt.Sprintf("TestClusterMember-%v", i),
			manager.ConfigClusterSecret: "test123",
		}, manager.NewMemStateInfo())
		cs = append(cs, ds)
	}

	return cs
}

/*
joinCluster joins up a given cluster.
*/
func joinCluster(cluster []*cluster.DistributedStorage, t *testing.T) {

	for i, dd := range cluster {
		dd.Start()
		defer dd.Close()

		if i > 0 {
			err := dd.MemberManager.JoinCluster(cluster[0].MemberManager.Name(),
				cluster[0].MemberManager.NetAddr())
			if err != nil {
				t.Error(err)
				return
			}
		}
	}
}

func checkStateInfo(mm *manager.MemberManager, expectedStateInfo string) error {
	var w bytes.Buffer

	ret := json.NewEncoder(&w)
	ret.Encode(mm.StateInfo().Map())

	out := bytes.Buffer{}

	err := json.Indent(&out, w.Bytes(), "", "  ")
	if err != nil {
		return err
	}

	if out.String() != expectedStateInfo {
		return fmt.Errorf("Unexpected state info: %v\nexpected: %v",
			out.String(), expectedStateInfo)
	}

	return nil
}
