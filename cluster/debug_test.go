/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package cluster

import (
	"math"
	"testing"
	"time"

	"devt.de/krotik/eliasdb/cluster/manager"
)

func TestDebugging(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 10
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Setup a cluster

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	// Log transfer worker runs

	logTransferWorker = true
	defer func() { logTransferWorker = false }()

	// Create a cluster with 3 members and a replication factor of 2

	cluster3, ms := createCluster(3, 2)

	// Debug output

	// manager.LogDebug = manager.LogInfo
	// log.SetOutput(os.Stderr)
	// defer func() { log.SetOutput(ioutil.Discard) }()

	for i, dd := range cluster3 {
		dd.Start()
		defer dd.Close()

		if i > 0 {
			err := dd.MemberManager.JoinCluster(cluster3[0].MemberManager.Name(), cluster3[0].MemberManager.NetAddr())
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	ClearMSMap()
	msmap[cluster3[1]] = ms[1]

	sm := cluster3[1].StorageManager("test", true)

	// Insert two strings into the store

	if loc, err := sm.Insert("test1"); loc != 1 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	if loc, err := sm.Insert("test2"); loc != 2 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	ms[1].transferRunning = true
	go func() {
		time.Sleep(10 * time.Millisecond)
		ms[1].transferRunning = false
	}()
	WaitForTransfer()

	if res := DumpMemoryClusterLayout("test"); res != `MemoryStorage: mgs2
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
` {
		t.Error("Unexpected result:", res)
		return
	}

}
