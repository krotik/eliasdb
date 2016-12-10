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
	"fmt"
	"strconv"
	"strings"

	"devt.de/eliasdb/cluster/manager"
	"devt.de/eliasdb/hash"
)

/*
MaxSizeRebalanceLists is the maximum size for rebalancing lists within one rebalance request.
*/
const MaxSizeRebalanceLists = 100

/*
runRebalanceWorker flag to switch off automatic rebalancing
*/
var runRebalanceWorker = true

/*
logRebalanceWorker flag to write a log message every time automatic rebalancing is running
*/
var logRebalanceWorker = false

/*
rebalanceHousekeepingInterval defines how often housekeeping needs to run before
a rebalance task is run.
*/
var rebalanceHousekeepingInterval = 180

/*
rebalanceWorker is the background thread which handles automatic rebalancing
when the configuration of the cluster changes or to autocorrect certain errors.
*/
func (ms *memberStorage) rebalanceWorker(forceRun bool) {

	// Make sure only one transfer task is running at a time and that
	// subsequent requests are not queued up

	ms.rebalanceLock.Lock()

	if !runRebalanceWorker || ms.rebalanceRunning {
		ms.rebalanceLock.Unlock()
		return
	}

	// Make sure rebalancing only runs every rebalanceHousekeepingInterval

	if !forceRun && ms.rebalanceCounter > 0 {
		ms.rebalanceCounter--
		ms.rebalanceLock.Unlock()

		return
	}

	ms.rebalanceCounter = rebalanceHousekeepingInterval
	ms.rebalanceRunning = true

	ms.rebalanceLock.Unlock()

	defer func() {
		ms.rebalanceLock.Lock()
		ms.rebalanceRunning = false
		ms.rebalanceLock.Unlock()
	}()

	if logRebalanceWorker {
		manager.LogDebug(ms.ds.Name(), "(RB): Running rebalance worker task")
	}

	distTable, err := ms.ds.DistributionTable()
	if err != nil {
		manager.LogDebug(ms.ds.Name(), "(RB): Cannot rebalance not operational cluster: ",
			err.Error())
		return
	}

	// Go through all maintained stuff and collect storage name, location and version

	it := hash.NewHTreeIterator(ms.at.translation)

	for it.HasNext() {
		chunks := MaxSizeRebalanceLists

		maintLocs := make([]uint64, 0, MaxSizeRebalanceLists)
		maintVers := make([]uint64, 0, MaxSizeRebalanceLists)
		maintMgmts := make([]string, 0, MaxSizeRebalanceLists)

		for it.HasNext() || chunks <= 0 {
			key, val := it.Next()

			if tr, ok := val.(*translationRec); ok {

				smname := strings.Split(string(key[len(transPrefix):]), "#")[0]
				cloc, _ := strconv.ParseUint(string(key[len(fmt.Sprint(transPrefix, smname, "#")):]), 10, 64)

				maintMgmts = append(maintMgmts, smname)
				maintLocs = append(maintLocs, cloc)
				maintVers = append(maintVers, tr.ver)
			}
		}

		// Send info about maintained stuff to all relevant members

		receiverMap := make(map[string]string)

		for _, cloc := range maintLocs {

			primary, replicas := distTable.LocationHome(cloc)

			members := make([]string, 0, len(replicas)+1)
			members = append(members, primary)
			members = append(members, replicas...)

			for _, member := range members {

				_, ok := receiverMap[member]

				if member == ms.ds.MemberManager.Name() || ok {
					continue
				}

				receiverMap[member] = ""

				request := &DataRequest{RTRebalance, map[DataRequestArg]interface{}{
					RPStoreName: maintMgmts,
					RPLoc:       maintLocs,
					RPVer:       maintVers,
					RPSrc:       ms.ds.MemberManager.Name(),
				}, nil, false}

				_, err := ms.ds.sendDataRequest(member, request)

				if err != nil {
					manager.LogDebug(ms.ds.Name(), "(RB): ",
						fmt.Sprintf("Member %v Error: %v", member, err))
				}
			}
		}
	}
}
