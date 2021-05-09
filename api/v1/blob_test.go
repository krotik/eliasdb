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
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/storage"
)

func TestBlob(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointBlob

	// Test error message

	_, _, res := sendTestRequest(queryURL, "GET", nil)

	if res != "Need a partition and a specific data ID" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL, "POST", nil)

	if res != "Need a partition" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL, "PUT", nil)

	if res != "Need a partition and a specific data ID" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL, "DELETE", nil)

	if res != "Need a partition and a specific data ID" {
		t.Error("Unexpected response:", res)
		return
	}

	queryURL = "http://localhost" + TESTPORT + EndpointBlob + "mypart/"

	_, _, res = sendTestRequest(queryURL+"a", "GET", nil)

	if res != "Could not decode data ID: strconv.ParseUint: parsing \"a\": invalid syntax" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"a", "PUT", nil)

	if res != "Could not decode data ID: strconv.ParseUint: parsing \"a\": invalid syntax" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"a", "DELETE", nil)

	if res != "Could not decode data ID: strconv.ParseUint: parsing \"a\": invalid syntax" {
		t.Error("Unexpected response:", res)
		return
	}

	// Test normal storage

	st, _, res := sendTestRequest(queryURL, "POST", []byte{0x0b, 0x00, 0x00, 0x0b, 0x01, 0x0e, 0x05})

	if st != "200 OK" || res != `
{
  "id": 1
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	msm := gmMSM.StorageManager("mypart"+StorageSuffixBlob, false)
	msm.(*storage.MemoryStorageManager).AccessMap[2] = storage.AccessInsertError

	st, _, res = sendTestRequest(queryURL, "POST", []byte{0x0b, 0x00, 0x00, 0x0b, 0x01, 0x0e, 0x05})

	if st != "500 Internal Server Error" || res != "Record is already in-use (<memory> - )" {
		t.Error("Unexpected response:", st, res)
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, 2)

	// Simulate a change miss

	msm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessNotInCache

	st, _, res = sendTestRequest(queryURL+"1", "GET", nil)

	if st != "200 OK" || fmt.Sprintf("%x", res) != "0b00000b010e05" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, 1)

	st, _, res = sendTestRequest(queryURL+"1", "GET", nil)

	if st != "200 OK" || fmt.Sprintf("%x", res) != "0b00000b010e05" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}

	msm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessUpdateError

	st, _, res = sendTestRequest(queryURL+"1", "PUT", []byte{0x0b, 0x0c})

	if st != "500 Internal Server Error" || res != "Slot not found (mystorage/mypart.blob - Location:1)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, 1)

	st, _, res = sendTestRequest(queryURL+"1", "PUT", []byte{0x0b, 0x0c})

	if st != "200 OK" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}

	st, _, res = sendTestRequest(queryURL+"1", "GET", nil)

	if st != "200 OK" || fmt.Sprintf("%x", res) != "0b0c" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}

	msm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessFreeError

	st, _, res = sendTestRequest(queryURL+"1", "DELETE", nil)

	if st != "500 Internal Server Error" || res != "Slot not found (mystorage/mypart.blob - Location:1)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, 1)

	st, _, res = sendTestRequest(queryURL+"1", "DELETE", nil)

	if st != "200 OK" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}

	st, _, res = sendTestRequest(queryURL+"1", "GET", nil)

	if st != "200 OK" || fmt.Sprintf("%x", res) != "" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}

	st, _, res = sendTestRequest(queryURL+"2", "GET", nil)

	if st != "200 OK" || fmt.Sprintf("%x", res) != "" {
		t.Error("Unexpected response:", st, fmt.Sprintf("%x", res))
		return
	}
}
