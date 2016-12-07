/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package manager

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

var consoleOutput = false
var liveOutput = false

type LogWriter struct {
	w io.Writer
}

func (l LogWriter) Write(p []byte) (n int, err error) {
	if liveOutput {
		fmt.Print(string(p))
	}
	return l.w.Write(p)
}

func TestMain(m *testing.M) {
	flag.Parse()

	// Create output capture file

	outFile, err := os.Create("out.txt")
	if err != nil {
		panic(err)
	}

	// Ensure logging is directed to the file

	log.SetOutput(LogWriter{outFile})

	// Create memberErrors map

	MemberErrors = make(map[string]error)
	MemberErrorExceptions = make(map[string][]string)

	// Disable housekeeping by default

	runHousekeeping = false
	defer func() { runHousekeeping = true }()

	// Run the tests

	res := m.Run()

	log.SetOutput(os.Stderr)

	// Collected output

	outFile.Sync()
	outFile.Close()

	stdout, err := ioutil.ReadFile("out.txt")
	if err != nil {
		panic(err)
	}

	// Handle collected output

	if consoleOutput {
		fmt.Println(string(stdout))
	}

	os.RemoveAll("out.txt")

	os.Exit(res)
}

/*
Create a cluster with n members (all storage is in memory)
*/
func createCluster(n int) []*MemberManager {

	var mms []*MemberManager

	for i := 0; i < n; i++ {
		mm := NewMemberManager(fmt.Sprintf("localhost:%v", 9020+i),
			fmt.Sprintf("TestClusterMember-%v", i), "test123", NewMemStateInfo())

		mm.SetEventHandler(func() {}, func() {})

		mms = append(mms, mm)
	}

	return mms
}

// Test network failure

type testNetError struct {
}

func (*testNetError) Error() string {
	return "test.net.Error"
}

func (*testNetError) Timeout() bool {
	return false
}

func (*testNetError) Temporary() bool {
	return true
}

type testDataReq struct {
	Test1 string
	Test2 interface{}
	Test3 map[string]interface{}
}

func TestDataRequest(t *testing.T) {

	// Debug logging

	// liveOutput = true
	// LogDebug = LogInfo
	// defer func() { liveOutput = false }()

	cluster2 := createCluster(2)

	cluster2[0].Start()
	cluster2[1].Start()
	defer cluster2[0].Shutdown()
	defer cluster2[1].Shutdown()

	// Join up the cluster

	cluster2[0].JoinCluster(cluster2[1].name, cluster2[1].Client.rpc)

	// Register test data request with gob

	gob.Register(&testDataReq{})

	// Register handler on one member

	var res *testDataReq

	testdata := &testDataReq{"123", []byte{1, 2, 3}, map[string]interface{}{
		"test1": 1.012,
		"test2": true,
		"test3": []string{"a", "b"},
	}}

	// Check that nothing goes wrong if no handler is installed

	reqres, err := cluster2[0].Client.SendDataRequest(cluster2[1].name, testdata)
	if err != nil || reqres != nil {
		t.Error(err)
		return
	}

	cluster2[1].SetHandleDataRequest(func(data interface{}, response *interface{}) error {
		res = data.(*testDataReq)
		*response = "testok"
		return nil
	})

	reqres, err = cluster2[0].Client.SendDataRequest(cluster2[1].name, testdata)
	if err != nil {
		t.Error(err)
		return
	} else if reqres != "testok" {
		t.Error("Unexpected request response:", reqres)
		return
	} else if res.Test1 != testdata.Test1 ||
		fmt.Sprint(res.Test2) != fmt.Sprint(testdata.Test2) ||
		fmt.Sprint(res.Test3["test1"]) != fmt.Sprint(testdata.Test3["test1"]) ||
		fmt.Sprint(res.Test3["test2"]) != fmt.Sprint(testdata.Test3["test2"]) ||
		fmt.Sprint(res.Test3["test3"]) != fmt.Sprint(testdata.Test3["test3"]) {
		t.Error("Data got changed while in transfer:", res, testdata)
		return
	}

	// Test error return

	cluster2[1].SetHandleDataRequest(func(data interface{}, response *interface{}) error {
		return errors.New("TestError")
	})

	_, err = cluster2[0].Client.SendDataRequest(cluster2[1].name, testdata)
	if err.Error() != "ClusterError: Member error (TestError)" {
		t.Error(err)
		return
	}
}

func TestCluster2MemberCluster(t *testing.T) {

	// Debug logging

	// liveOutput = true
	// LogDebug = LogInfo
	// defer func() { liveOutput = false }()

	cluster2 := createCluster(2)

	cluster2[0].Start()
	cluster2[1].Start()
	defer cluster2[0].Shutdown()
	defer cluster2[1].Shutdown()

	// Join up the cluster

	cluster2[0].JoinCluster(cluster2[1].name, cluster2[1].Client.rpc)

	// Check state info

	if err := checkStateInfo(cluster2[0], `
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
	} else if err := checkStateInfo(cluster2[1], `
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

	// Break up the cluster - let a member eject itself

	cluster2[0].EjectMember(cluster2[0].name)

	if err := checkStateInfo(cluster2[0], `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
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
	} else if err := checkStateInfo(cluster2[1], `
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

	// Join up the cluster again

	cluster2[1].JoinCluster(cluster2[0].name, cluster2[0].Client.rpc)

	if err := checkStateInfo(cluster2[0], `
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
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster2[1], `
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
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Break up the cluster - eject the other member
	// the state on the other member is not updated

	cluster2[0].EjectMember(cluster2[1].name)

	if err := checkStateInfo(cluster2[0], `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "5"
  ],
  "tsold": [
    "TestClusterMember-0",
    "4"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster2[1], `
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
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Try to rejoin from a member of the cluster - the member which did not update
	// its state should decline this as it thinks it is still part of the cluster

	cluster2[0].JoinCluster(cluster2[1].name, cluster2[1].Client.rpc)

	if err := checkStateInfo(cluster2[0], `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "5"
  ],
  "tsold": [
    "TestClusterMember-0",
    "4"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster2[1], `
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
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Join up the cluster one last time from the member which did not update its
	// state - all should be well afterwards ...

	cluster2[1].JoinCluster(cluster2[0].name, cluster2[0].Client.rpc)

	if err := checkStateInfo(cluster2[0], `
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
    "6"
  ],
  "tsold": [
    "TestClusterMember-0",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster2[1], `
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
    "6"
  ],
  "tsold": [
    "TestClusterMember-0",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}
}

func TestMemberInfo(t *testing.T) {

	cluster3 := createCluster(3)

	for i, member := range cluster3 {

		err := member.Start()
		defer member.Shutdown()

		if err != nil {
			t.Error(err)
			return
		}

		if i > 0 {

			// Join up the cluster - members 1, 2 join member 0

			if err := member.JoinCluster(cluster3[0].name,
				cluster3[0].Client.rpc); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Simulate failure of member 2

	MemberErrors[cluster3[2].name] = &testNetError{}

	// Reset error maps

	defer func() {
		MemberErrors = make(map[string]error)
	}()

	// Set member info on the members

	cluster3[0].MemberInfo()["123"] = "v123"
	cluster3[1].MemberInfo()["456"] = "v456"
	cluster3[2].MemberInfo()["789"] = "v789"

	// Request all member infos

	mi := cluster3[0].MemberInfoCluster()

	var w bytes.Buffer

	ret := json.NewEncoder(&w)
	ret.Encode(mi)

	out := bytes.Buffer{}

	err := json.Indent(&out, w.Bytes(), "", "  ")
	if err != nil {
		t.Error(err)
		return
	}

	expectedClusterMemberInfo := `
{
  "TestClusterMember-0": {
    "123": "v123"
  },
  "TestClusterMember-1": {
    "456": "v456"
  },
  "TestClusterMember-2": {
    "error": "ClusterError: Network error (test.net.Error)"
  }
}
`[1:]

	if out.String() != expectedClusterMemberInfo {
		t.Errorf("Unexpected cluster member info: %v\nexpected: %v",
			out.String(), expectedClusterMemberInfo)
	}
}

func TestClusterHouseKeeping(t *testing.T) {

	var log []string

	origLogDebug := LogDebug
	LogDebug = func(v ...interface{}) {
		log = append(log, fmt.Sprint(v...))
	}
	defer func() {
		LogDebug = origLogDebug
	}()

	c := createCluster(1)[0]

	// Activate housekeeping for this test

	oldRunHousekeeping := runHousekeeping
	oldFreqHousekeeping := FreqHousekeeping
	runHousekeeping = true
	logHousekeeping = true
	FreqHousekeeping = 10
	defer func() {
		runHousekeeping = oldRunHousekeeping
		FreqHousekeeping = oldFreqHousekeeping
		logHousekeeping = false
	}()

	c.Start()

	time.Sleep(60 * time.Millisecond)

	c.Shutdown()

	hkCount := 0
	for _, l := range log {
		if strings.Contains(l, "(HK): Running housekeeping task") {
			hkCount++
		}
	}

	if hkCount < 3 {
		t.Error("Unexpected count of housekeeping thread runs:", hkCount)
	}

	// Test shutting down a member twice

	if err := c.Shutdown(); err != nil {
		t.Error("Unexpected result", err)
		return
	} else if log[len(log)-1] != "Member manager TestClusterMember-0 already shut down" {
		t.Error("Unexpected result", err)
		return
	}
}

func TestClusterEjection(t *testing.T) {
	var err error

	cluster3 := createCluster(3)

	for i, member := range cluster3 {

		err := member.Start()
		defer member.Shutdown()

		if err != nil {
			t.Error(err)
			return
		}

		if i > 0 {

			// Join up the cluster - members 1, 2 join member 0

			if err := member.JoinCluster(cluster3[0].name,
				cluster3[0].Client.rpc); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Debug logging

	// liveOutput = true
	// LogDebug = LogInfo
	// defer func() { liveOutput = false }()

	// Try to double join a member

	err = cluster3[1].JoinNewMember(cluster3[2].Name(), cluster3[2].Client.rpc)
	if err.Error() != "ClusterError: Cluster configuration error (Cannot add member TestClusterMember-2 as a member with the same name exists already)" {
		t.Error("Unexpected result:", err)
		return
	}

	if err := checkStateInfo(cluster3[2], `
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
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Simulate failure of member 2

	MemberErrors[cluster3[2].name] = &testNetError{}

	// Reset error maps

	defer func() {
		MemberErrors = make(map[string]error)
	}()

	cluster3[0].StopHousekeeping = true
	cluster3[0].HousekeepingWorker()
	cluster3[0].StopHousekeeping = false

	cluster3[0].HousekeepingWorker()

	if fp := fmt.Sprint(cluster3[0].Client.FailedPeers()); fp != "[TestClusterMember-2]" {
		t.Error("Unexpected result:", fp)
		return
	}

	if err := checkStateInfo(cluster3[0], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster3[2], `
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
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Now eject member 2 from the cluster via member 1

	if err := cluster3[0].Client.SendEjectMember(
		cluster3[1].name, cluster3[2].name); err != nil {
		t.Error(err)
		return
	}

	if err := checkStateInfo(cluster3[0], `
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
    "TestClusterMember-1",
    "5"
  ],
  "tsold": [
    "TestClusterMember-0",
    "4"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Now member 2 comes back

	MemberErrors = make(map[string]error)

	// Requests which require cluster membership should now fail

	err = cluster3[2].Client.SendAcquireClusterLock("123")
	if err.Error() != "ClusterError: Member error (Client is not a cluster member)" {
		t.Error(err)
		return
	}

	// Member detect that it was ejected

	cluster3[2].HousekeepingWorker()

	if err := checkStateInfo(cluster3[2], `
{
  "failed": [
    "TestClusterMember-0",
    "Client is not a cluster member",
    "TestClusterMember-1",
    "Client is not a cluster member"
  ],
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
    "TestClusterMember-2",
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Join member 2 again

	if err := cluster3[2].JoinCluster(cluster3[0].name,
		cluster3[0].Client.rpc); err != nil {
		t.Error(err)
		return
	}

	cluster3[2].HousekeepingWorker()

	if err := checkStateInfo(cluster3[2], `
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
    "6"
  ],
  "tsold": [
    "TestClusterMember-1",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster3[1], `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "6"
  ],
  "tsold": [
    "TestClusterMember-1",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}
}

func TestClusterTemporaryFailure(t *testing.T) {

	cluster4 := createCluster(5)

	// Start and join the members and ensure they are shut down after the test finishes

	for i, member := range cluster4 {

		err := member.Start()
		defer member.Shutdown()

		if err != nil {
			t.Error(err)
			return
		}

		if i > 0 && i < 4 {

			// Join up the cluster - members 1, 2, 3 join member 0 - member 4 stays on its own

			if err := member.JoinCluster(cluster4[0].name,
				cluster4[0].Client.rpc); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Debug logging

	// liveOutput = true
	// LogDebug = LogInfo
	// defer func() { liveOutput = false }()

	// Simulate network partitioning (Member 0 and 1 can talk and
	// member 2, 3 and 4 can talk)

	MemberErrors[cluster4[0].name] = &testNetError{}
	MemberErrors[cluster4[1].name] = &testNetError{}
	MemberErrorExceptions[cluster4[0].name] = []string{cluster4[1].name}
	MemberErrorExceptions[cluster4[1].name] = []string{cluster4[0].name}

	MemberErrors[cluster4[2].name] = &testNetError{}
	MemberErrors[cluster4[3].name] = &testNetError{}
	MemberErrorExceptions[cluster4[2].name] = []string{cluster4[3].name, cluster4[4].name}
	MemberErrorExceptions[cluster4[3].name] = []string{cluster4[2].name, cluster4[4].name}
	MemberErrorExceptions[cluster4[4].name] = []string{cluster4[2].name, cluster4[3].name}

	// Reset error maps

	defer func() {
		MemberErrors = make(map[string]error)
		MemberErrorExceptions = make(map[string][]string)
	}()

	if err := checkStateInfo(cluster4[1], `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-3",
    "localhost:9023"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster4[2], `
{
  "failed": null,
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-3",
    "localhost:9023"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "4"
  ],
  "tsold": [
    "TestClusterMember-0",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	//  Simulate housekeeping on all members

	cluster4[0].HousekeepingWorker()
	cluster4[1].HousekeepingWorker()
	cluster4[2].HousekeepingWorker()
	cluster4[3].HousekeepingWorker()

	// Send invalid add new member from a (simulated) pure client

	rpcbak := cluster4[4].Client.rpc
	cluster4[4].Client.rpc = ""

	err := cluster4[4].JoinCluster(cluster4[3].name, "")
	if err.Error() != "ClusterError: Cluster configuration error (Cannot add member without RPC interface)" {
		t.Error(err)
		return
	}

	cluster4[4].Client.rpc = rpcbak

	// Add a new member

	if err := cluster4[4].JoinCluster(cluster4[3].name,
		cluster4[3].Client.rpc); err != nil {
		t.Error(err)
		return
	}

	// Check lists

	if ml := fmt.Sprint(cluster4[0].Members()); ml != "[TestClusterMember-0 TestClusterMember-1 TestClusterMember-2 TestClusterMember-3]" {
		t.Error("Unexpected members list:", ml)
		return
	}

	if ft := cluster4[0].Client.FailedTotal(); ft != 2 || !cluster4[0].Client.IsFailed(cluster4[2].name) || !cluster4[0].Client.IsFailed(cluster4[3].name) {
		t.Error("Unexpected failed total:", ft)
		return
	}

	// Member 0 and 1 think that member 2 and 3 are not reachable and vice versa
	// There is now a conflicting cluster state from both network partitions

	if err := checkStateInfo(cluster4[0], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error",
    "TestClusterMember-3",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-3",
    "localhost:9023"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "5"
  ],
  "tsold": [
    "TestClusterMember-0",
    "4"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster4[1], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error",
    "TestClusterMember-3",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-3",
    "localhost:9023"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "5"
  ],
  "tsold": [
    "TestClusterMember-0",
    "4"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster4[2], `
{
  "failed": [
    "TestClusterMember-0",
    "test.net.Error",
    "TestClusterMember-1",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-3",
    "localhost:9023",
    "TestClusterMember-4",
    "localhost:9024"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-3",
    "6"
  ],
  "tsold": [
    "TestClusterMember-2",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster4[3], `
{
  "failed": [
    "TestClusterMember-0",
    "test.net.Error",
    "TestClusterMember-1",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-3",
    "localhost:9023",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-4",
    "localhost:9024"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-3",
    "6"
  ],
  "tsold": [
    "TestClusterMember-2",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Remove the network partitions

	MemberErrors = make(map[string]error)
	MemberErrorExceptions = make(map[string][]string)

	// Simulate housekeeping on member 0 kicks in first

	cluster4[0].HousekeepingWorker()

	if err := checkStateInfo(cluster4[0], `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-3",
    "localhost:9023",
    "TestClusterMember-4",
    "localhost:9024"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "6"
  ],
  "tsold": [
    "TestClusterMember-0",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster4[2], `
{
  "failed": null,
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-3",
    "localhost:9023",
    "TestClusterMember-4",
    "localhost:9024"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "6"
  ],
  "tsold": [
    "TestClusterMember-0",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster4[4], `
{
  "failed": null,
  "members": [
    "TestClusterMember-4",
    "localhost:9024",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-3",
    "localhost:9023"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-0",
    "6"
  ],
  "tsold": [
    "TestClusterMember-0",
    "5"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}
}

func TestClusterBuilding(t *testing.T) {

	cluster3 := createCluster(3)

	// Start the members and ensure they are shut down after the test finishes

	// Debug logging

	// liveOutput = true
	// LogDebug = LogInfo

	for _, member := range cluster3 {

		err := member.Start()
		defer member.Shutdown()

		if err != nil {
			t.Error(err)
			return
		}
	}

	// defer func() { liveOutput = false }()

	// Check state info

	if err := checkStateInfo(cluster3[1], `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-1",
    "1"
  ],
  "tsold": [
    "",
    "0"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Form the cluster by adding member 2 into the cluster of member 1

	err := cluster3[2].JoinCluster(cluster3[1].name,
		cluster3[1].Client.rpc)
	if err != nil {
		t.Error(err)
		return
	}

	// Check state info

	if err := checkStateInfo(cluster3[1], `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022"
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
	} else if err := checkStateInfo(cluster3[2], `
{
  "failed": null,
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-1",
    "localhost:9021"
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

	// Simulate member 2 becomes unavailable

	MemberErrors[cluster3[2].name] = &testNetError{}
	defer delete(MemberErrors, cluster3[2].name)

	// Join member 0 via member 2

	err = cluster3[0].JoinCluster(cluster3[2].name,
		cluster3[2].Client.rpc)
	if err.Error() != "ClusterError: Network error (test.net.Error)" {
		t.Error(err)
		return
	}

	// Join member 0 via member 1

	err = cluster3[0].JoinCluster(cluster3[1].name,
		cluster3[1].Client.rpc)
	if err != nil {
		t.Error(err)
		return
	}

	// Check state info - Member 1 knows now that member 2 has failed

	if err := checkStateInfo(cluster3[1], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022"
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
	} else if err := checkStateInfo(cluster3[0], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022"
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

	// Simulate member 2 becomes available again

	delete(MemberErrors, cluster3[2].name)

	// Member 2 has still an old state info

	if err := checkStateInfo(cluster3[2], `
{
  "failed": null,
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-1",
    "localhost:9021"
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

	// Member 2 should be updated the state info eventually through housekeeping

	cluster3[2].HousekeepingWorker()

	// Member 2 is still considered failed by the cluster

	if err := checkStateInfo(cluster3[2], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error"
  ],
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
	} else if err := checkStateInfo(cluster3[1], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022"
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
	} else if err := checkStateInfo(cluster3[0], `
{
  "failed": [
    "TestClusterMember-2",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022"
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

	// Now housekeeping runs on member 1 which should detect that 2 is back
	// again - the state info on all members should be updated

	cluster3[1].HousekeepingWorker()

	if err := checkStateInfo(cluster3[2], `
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
    "TestClusterMember-1",
    "4"
  ],
  "tsold": [
    "TestClusterMember-1",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster3[1], `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-1",
    "4"
  ],
  "tsold": [
    "TestClusterMember-1",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	} else if err := checkStateInfo(cluster3[0], `
{
  "failed": null,
  "members": [
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-2",
    "localhost:9022"
  ],
  "replication": 1,
  "ts": [
    "TestClusterMember-1",
    "4"
  ],
  "tsold": [
    "TestClusterMember-1",
    "3"
  ]
}
`[1:]); err != nil {
		t.Error(err)
		return
	}
}

func checkStateInfo(mm *MemberManager, expectedStateInfo string) error {
	var w bytes.Buffer

	ret := json.NewEncoder(&w)
	ret.Encode(mm.stateInfo.Map())

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

func TestStateInfo(t *testing.T) {

	cluster1 := createCluster(1)

	cluster1[0].Client.peers["abc"] = "localhost:123"
	cluster1[0].Client.peers["def"] = "localhost:124"

	cluster1[0].updateStateInfo(true)

	si := cluster1[0].stateInfo

	sip, _ := si.Get(StateInfoMEMBERS)
	if fmt.Sprint(sip) != "[TestClusterMember-0 localhost:9020 abc localhost:123 def localhost:124]" {
		t.Error("Unexpected StateInfo:", sip)
		return
	}

	sif, _ := si.Get(StateInfoFAILED)
	if fmt.Sprint(sif) != "[]" {
		t.Error("Unexpected StateInfo:", sif)
		return
	}

	cluster1[0].stateInfo = NewMemStateInfo()
	cluster1[0].Client.peers = nil

	cluster1[0].applyStateInfo(si.(*MemStateInfo).data)

	if len(cluster1[0].stateInfo.(*MemStateInfo).data) != 5 {
		t.Error("State info not correct: ", cluster1[0].stateInfo.(*MemStateInfo).data)
		return
	}

	peers := cluster1[0].Client.peers
	if len(peers) != 2 || peers["abc"] != "localhost:123" || peers["def"] != "localhost:124" {
		t.Error("Unexpected peers map:", peers)
		return
	}

	// Create a new member manager and apply a given state info

	mm := NewMemberManager(fmt.Sprintf("localhost:9022"),
		"TestClusterMember-9", "test123", cluster1[0].stateInfo)

	if err := checkStateInfo(mm, `
{
  "failed": null,
  "members": [
    "TestClusterMember-9",
    "localhost:9022",
    "TestClusterMember-0",
    "localhost:9020",
    "abc",
    "localhost:123",
    "def",
    "localhost:124"
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
}
`[1:]); err != nil {
		t.Error(err)
		return
	}
}

func TestLowLevelManagerCommunication(t *testing.T) {

	cluster3 := createCluster(3)

	// Try starting with an invalid rpc

	origRPC := cluster3[0].Client.rpc
	cluster3[0].Client.rpc = ":-1"
	if err := cluster3[0].Start(); !strings.HasPrefix(err.Error(), "listen tcp") {
		t.Error("Unexpected result:", err)
		return
	}
	cluster3[0].Client.rpc = origRPC

	// Start the cluster and ensure it is shut down after the test finishes

	for _, member := range cluster3 {

		err := member.Start()
		defer member.Shutdown()

		if err != nil {
			t.Error(err)
			return
		}
	}

	// Check info of MemberManager

	if res := cluster3[1].Name(); res != cluster3[1].name {
		t.Error("Unexpected result:", res)
		return
	} else if res := cluster3[1].NetAddr(); res != cluster3[1].Client.rpc {
		t.Error("Unexpected result:", res)
		return
	} else if res := cluster3[1].StateInfo(); res != cluster3[1].stateInfo {
		t.Error("Unexpected result:", res)
		return
	}

	// Do a ping which add temrorary a member

	pres, err := cluster3[0].Client.SendPing(cluster3[1].Name(), cluster3[1].Client.rpc)
	if err != nil || fmt.Sprint(pres) != "[Pong]" {
		t.Error("Unexpected result:", pres, err)
		return
	}

	// Manually add some peers

	cluster3[0].Client.peers[cluster3[1].Name()] = cluster3[1].Client.rpc
	cluster3[1].Client.peers[cluster3[1].Name()] = cluster3[1].Client.rpc

	// Add invalid entry

	cluster3[0].Client.peers["bla"] = "localhost:-1"

	_, err = cluster3[0].Client.SendRequest("bla", RPCPing, nil)
	if !strings.HasPrefix(err.Error(), "ClusterError: Network error") {
		t.Error("Unexpected result:", err.Error())
		return
	}

	// Send ping (at this point member 0 is unknown to member 1 so it is treated as a pure client)

	pres, err = cluster3[0].Client.SendPing(cluster3[1].Name(), "")

	if err != nil || fmt.Sprint(pres) != "[Pong]" {
		t.Error("Unexpected ping result:", pres, err)
		return
	}

	// Send ping with unknown target - fail is client side

	pres, err = cluster3[0].Client.SendPing(cluster3[1].Name()+"123", "")

	if err.Error() != "ClusterError: Unknown peer member (TestClusterMember-1123)" || pres != nil {
		t.Error("Unexpected ping result:", pres, err)
		return
	}

	// Send ping with unknown target - fail is server side

	res, err := cluster3[0].Client.SendRequest(cluster3[1].Name(),
		RPCPing, map[RequestArgument]interface{}{
			RequestTARGET: cluster3[1].Name() + "123",
		})

	if err.Error() != "ClusterError: Member error (Unknown target member)" || res != nil {
		t.Error("Unexpected ping result:", res, err)
		return
	}

	// Send ping with invalid member token

	oldAuth := cluster3[0].Client.token.MemberAuth
	cluster3[0].Client.token.MemberAuth = oldAuth + "123"

	pres, err = cluster3[0].Client.SendPing(cluster3[1].Name(), "")

	if err.Error() != "ClusterError: Member error (Invalid member token)" || pres != nil {
		t.Error("Unexpected ping result:", pres, err)
		return
	}

	cluster3[0].Client.token.MemberAuth = oldAuth

	// Test acquisition of a cluster lock

	res, err = cluster3[1].Client.SendRequest(cluster3[1].Name(),
		RPCAcquireLock, map[RequestArgument]interface{}{
			RequestTARGET: cluster3[1].Name(),
			RequestLOCK:   "mylock",
		})

	if err != nil || res != cluster3[1].Name() {
		t.Error(err, res)
		return
	}

	// Check that the lock was set

	if l := cluster3[1].Client.clusterLocks.Size(); l != 1 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l, _ := cluster3[1].Client.clusterLocks.Get("mylock"); l != cluster3[1].Name() {
		t.Error("Unexpected cluster lock owner:", l)
		return
	}

	// Try to acquire the lock for a different member

	res, err = cluster3[0].Client.SendRequest(cluster3[1].Name(),
		RPCAcquireLock, map[RequestArgument]interface{}{
			RequestTARGET: cluster3[1].Name(),
			RequestLOCK:   "mylock",
		})

	// Check the cluster member check

	if err.Error() != "ClusterError: Member error (Client is not a cluster member)" || res != nil {
		t.Error(err, res)
		return
	}

	err = cluster3[0].JoinNewMember(cluster3[1].Name(), cluster3[1].Client.rpc)

	if err.Error() != "ClusterError: Member error (Client is not a cluster member)" || res != nil {
		t.Error(err, res)
		return
	}

	// Register member 0 on member 1

	cluster3[1].Client.peers[cluster3[0].Name()] = cluster3[0].Client.rpc

	res, err = cluster3[0].Client.SendRequest(cluster3[1].Name(),
		RPCAcquireLock, map[RequestArgument]interface{}{
			RequestTARGET: cluster3[1].Name(),
			RequestLOCK:   "mylock",
		})

	if err.Error() != "ClusterError: Member error (ClusterError: Requested lock is already taken (TestClusterMember-1))" || res != nil {
		t.Error(err, res)
		return
	}

	// Release a lock from a wrong member

	res, err = cluster3[0].Client.SendRequest(cluster3[1].Name(),
		RPCReleaseLock, map[RequestArgument]interface{}{
			RequestTARGET: cluster3[1].Name(),
			RequestLOCK:   "mylock",
		})

	if err.Error() != "ClusterError: Member error (ClusterError: Requested lock not owned (Owned by TestClusterMember-1 not by TestClusterMember-0))" || res != nil {
		t.Error(err, res)
		return
	}

	// Check that the lock was not unset

	if l := cluster3[1].Client.clusterLocks.Size(); l != 1 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Release the lock from the correct member

	res, err = cluster3[1].Client.SendRequest(cluster3[1].Name(),
		RPCReleaseLock, map[RequestArgument]interface{}{
			RequestTARGET: cluster3[1].Name(),
			RequestLOCK:   "mylock",
		})

	if err != nil || res != nil {
		t.Error(err, res)
		return
	}

	// Check that the lock was unset

	if l := cluster3[1].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Register member 2 on member 1 and vice versa

	cluster3[1].Client.peers[cluster3[2].Name()] = cluster3[2].Client.rpc
	cluster3[2].Client.peers[cluster3[1].Name()] = cluster3[1].Client.rpc

	// Test taking lock with serious error - member which takes the lock should release
	// the ones which were already taken

	MemberErrors[cluster3[2].name] = errors.New("testerror")
	defer delete(MemberErrors, cluster3[2].name)

	err = cluster3[1].Client.SendAcquireClusterLock("123")
	if err.Error() != "ClusterError: Member error (testerror)" {
		t.Error("Test error expected:", err)
		return
	}

	// Check that the lock is not set

	if l := cluster3[1].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l := cluster3[0].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l := cluster3[2].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Check state info error

	_, err = cluster3[1].Client.SendStateInfoRequest(cluster3[2].name)
	if err.Error() != "ClusterError: Member error (testerror)" {
		t.Error("Unexpected result:", res, err)
		return
	}

	delete(MemberErrors, cluster3[2].name)

	// Use client function to take lock

	err = cluster3[0].Client.SendAcquireClusterLock("123")
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the lock was set

	if l := cluster3[1].Client.clusterLocks.Size(); l != 1 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l := cluster3[0].Client.clusterLocks.Size(); l != 1 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Use client to unlock

	err = cluster3[1].Client.SendReleaseClusterLock("123")
	if err.Error() != "ClusterError: Member error (ClusterError: Requested lock not owned (Owned by TestClusterMember-0 not by TestClusterMember-1))" {
		t.Error(err)
		return
	}

	err = cluster3[0].Client.SendReleaseClusterLock("123")
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the lock is not set

	if l := cluster3[1].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l := cluster3[0].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Remove wrong peer 1 entry from member 1

	delete(cluster3[1].Client.peers, cluster3[1].Name())

	// Acquire cluster lock for updating the state info

	if err := cluster3[1].Client.SendAcquireClusterLock(ClusterLockUpdateStateInfo); err != nil {
		t.Error(err)
		return
	}

	// Try to update the state info

	err = cluster3[0].UpdateClusterStateInfo()
	if err.Error() != "ClusterError: Member error (ClusterError: Requested lock is already taken (TestClusterMember-1))" {
		t.Error(err)
		return
	}

	// Try to eject member 1

	err = cluster3[1].EjectMember(cluster3[1].Name())

	if err.Error() != "ClusterError: Member error (ClusterError: Requested lock is already taken (TestClusterMember-1))" || res != nil {
		t.Error(err, res)
		return
	}

	// Release cluster lock for updating the state info

	if err := cluster3[1].Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo); err != nil {
		t.Error(err)
		return
	}

	// Simulate a write error while persisting the cluster state info

	MsiRetFlush = errors.New("TestFlushError")

	err = cluster3[1].EjectMember(cluster3[1].Name())

	MsiRetFlush = nil

	if err.Error() != "ClusterError: Member error (TestFlushError)" {
		t.Error(err)
		return
	}

	// Check that the lock is not left behindt

	if l := cluster3[1].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l := cluster3[0].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Check peer is still there

	_, ok := cluster3[0].Client.peers[cluster3[1].Name()]
	if !ok {
		t.Error("Expected member was not in peer list:", cluster3[1].Name())
		return
	}

	// Try to update the cluster state info

	MsiRetFlush = errors.New("TestFlushError")

	err = cluster3[0].UpdateClusterStateInfo()

	MsiRetFlush = nil

	if err.Error() != "TestFlushError" {
		t.Error(err)
		return
	}

	// Check that the lock is not left behindt

	if l := cluster3[1].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	} else if l := cluster3[0].Client.clusterLocks.Size(); l != 0 {
		t.Error("Unexpected cluster locks structure:", l)
		return
	}

	// Actually remove the member

	err = cluster3[1].EjectMember(cluster3[1].Name())

	if err != nil {
		t.Error(err)
		return
	}

	_, ok = cluster3[0].Client.peers[cluster3[1].Name()]
	if ok {
		t.Error("Unexpected member was in peer list:", cluster3[1].Name())
		return
	}
}
