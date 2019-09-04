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
	"fmt"
	"strconv"
	"strings"
)

/*
runHouseKeeping flag to switch off automatic start of housekeeping
*/
var runHousekeeping = true

/*
FreqHousekeeping is the frequency of running housekeeping tasks (ms)
*/
var FreqHousekeeping float64 = 1000

/*
logHousekeeping flag to write a log message every time the housekeeping task is running
*/
var logHousekeeping = false

/*
HousekeepingWorker is the background thread which handles various tasks to provide
"eventual" consistency for the cluster.
*/
func (mm *MemberManager) HousekeepingWorker() {

	mm.housekeepingLock.Lock()
	defer mm.housekeepingLock.Unlock()

	if mm.StopHousekeeping {
		return
	} else if logHousekeeping {
		LogDebug(mm.name, "(HK): Running housekeeping task")
	}

	// Special function which ensures that the given member is removed from the
	// failed list.

	removeFromFailedState := func(peer string) {

		mm.Client.maplock.Lock()
		defer mm.Client.maplock.Unlock()

		if _, ok := mm.Client.failed[peer]; ok {

			// Remove a member from the failed state list and send an update

			LogDebug(mm.name, "(HK): ",
				fmt.Sprintf("Removing %v from list of failed members", peer))

			delete(mm.Client.failed, peer)
		}
	}

	// Housekeeping will try to talk to all peers

	resolveConflict := false // Flag to resolve a state conflict at the end of a cycle.

	for peer := range mm.Client.peers {

		LogDebug(mm.name, "(HK): ",
			fmt.Sprintf("Housekeeping talking to: %v", peer))

		// Send a ping to the member

		res, err := mm.Client.SendPing(peer, "")

		if err != nil {
			LogDebug(mm.name, "(HK): ",
				fmt.Sprintf("Error pinging %v - %v", peer, err))
			continue

		} else if len(res) == 1 {
			LogDebug(mm.name, "(HK): ",
				fmt.Sprintf("Member %v says this instance is not part of the cluster", peer))

			mm.Client.maplock.Lock()
			mm.Client.failed[peer] = ErrNotMember.Error()
			mm.Client.maplock.Unlock()

			continue
		}

		// Check timestamp on the result and see where this member is:

		peerTsMember := res[1]
		peerTsTS, _ := strconv.ParseInt(res[2], 10, 64)
		peerTsOldMember := res[3]
		peerTsOldTS, _ := strconv.ParseInt(res[4], 10, 64)

		simmTS, _ := mm.stateInfo.Get(StateInfoTS)
		mmTS := simmTS.([]string)
		simmOldTS, _ := mm.stateInfo.Get(StateInfoTSOLD)
		mmOldTS := simmOldTS.([]string)

		mmTsMember := mmTS[0]
		mmTsTS, _ := strconv.ParseInt(mmTS[1], 10, 64)
		mmTsOldMember := mmOldTS[0]
		mmTsOldTS, _ := strconv.ParseInt(mmOldTS[1], 10, 64)

		LogDebug(mm.name, "(HK): ",
			fmt.Sprintf("TS Me  : Curr:%v:%v - Old:%v:%v", mmTsMember, mmTsTS, mmTsOldMember, mmTsOldTS))
		LogDebug(mm.name, "(HK): ",
			fmt.Sprintf("TS Peer: Curr:%v:%v - Old:%v:%v", peerTsMember, peerTsTS, peerTsOldMember, peerTsOldTS))

		if peerTsTS > mmTsTS || peerTsMember != mmTsMember {

			// Peer has a newer version

			if peerTsMember == mmTsMember && peerTsOldMember == mmTsMember && peerTsOldTS == mmTsTS {

				// Peer has the next state info version - update the local state info

				sf, err := mm.Client.SendStateInfoRequest(peer)

				if err == nil {
					LogDebug(mm.name, ": Updating state info of member")
					mm.applyStateInfo(sf)
				}

			} else {

				// Peer has a different version - potential conflict send a
				// state update at the end of the cycle

				if sf, err := mm.Client.SendStateInfoRequest(peer); err == nil {

					LogDebug(mm.name, ": Merging members in state infos")

					// Add any newly known cluster members

					mm.applyStateInfoPeers(sf, false)

					resolveConflict = true
				}
			}

			// Remove the member from the failed state list if it is on there

			removeFromFailedState(peer)

		} else if peerTsTS == mmTsTS && peerTsMember == mmTsMember {

			// Peer is up-to-date - check if it is in a failed state list

			removeFromFailedState(peer)
		}

		// We do nothing with members using an outdated cluster state
		// they should update eventually through their own housekeeping
	}

	// Check if there is a new failed members list

	sfFailed, _ := mm.stateInfo.Get(StateInfoFAILED)

	if len(sfFailed.([]string))/2 != len(mm.Client.failed) || resolveConflict {

		LogDebug(mm.name, "(HK): ",
			fmt.Sprintf("Updating other members with current failed members list: %v",
				strings.Join(mm.Client.FailedPeerErrors(), ", ")))

		if err := mm.UpdateClusterStateInfo(); err != nil {

			// Just update local state info if we could not update the peers

			LogDebug(mm.name, "(HK): ",
				fmt.Sprintf("Could not update cluster state: %v", err.Error()))

			mm.updateStateInfo(true)
		}
	}

	// Notify others that housekeeping has finished

	mm.notifyHouseKeeping()
}
