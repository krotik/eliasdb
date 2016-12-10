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

	"devt.de/common/timeutil"
	"devt.de/eliasdb/cluster/manager"
	"devt.de/eliasdb/hash"
)

/*
runTransferWorker flag to switch off transfer record processing
*/
var runTransferWorker = true

/*
logTransferWorker flag to write a log message every time the transfer worker task is running
*/
var logTransferWorker = false

/*
transferWorker is the background thread which handles various tasks to provide
"eventual" consistency for the cluster storage.
*/
func (ms *memberStorage) transferWorker() {

	// Make sure only one transfer task is running at a time and that
	// subsequent requests are not queued up

	ms.transferLock.Lock()

	if !runTransferWorker || ms.transferRunning {
		ms.transferLock.Unlock()
		return
	}

	ms.transferRunning = true

	ms.transferLock.Unlock()

	defer func() {
		ms.transferLock.Lock()
		ms.transferRunning = false
		ms.transferLock.Unlock()
	}()

	if logTransferWorker {
		manager.LogDebug(ms.ds.Name(), "(TR): Running transfer worker task")
	}

	// Go through the transfer table and try to process the tasks

	var processed [][]byte

	it := hash.NewHTreeIterator(ms.at.transfer)

	for it.HasNext() {
		key, val := it.Next()

		if val != nil {
			tr := val.(*transferRec)
			ts, _ := timeutil.TimestampString(string(key), "UTC")

			manager.LogDebug(ms.ds.Name(), "(TR): ",
				fmt.Sprintf("Processing transfer request %v for %v from %v",
					tr.request.RequestType, tr.members, ts))

			// Send the request to all members

			var failedMembers []string

			for _, member := range tr.members {

				if _, err := ms.ds.sendDataRequest(member, tr.request); err != nil {
					manager.LogDebug(ms.ds.Name(), "(TR): ",
						fmt.Sprintf("Member %v Error: %v", member, err))

					failedMembers = append(failedMembers, member)
				}
			}

			// Update or remove the translation record

			if len(failedMembers) == 0 {
				processed = append(processed, key)
			} else if len(failedMembers) < len(tr.members) {
				tr.members = failedMembers
				ms.at.transfer.Put(key, tr)
			}
		}
	}

	// Remove all processed transfer requests

	for _, key := range processed {
		ms.at.transfer.Remove(key)
	}

	// Flush the local storage

	ms.gs.FlushAll()

	// Trigger the rebalancing task - the task will only execute if it is time

	go ms.rebalanceWorker(false)
}
