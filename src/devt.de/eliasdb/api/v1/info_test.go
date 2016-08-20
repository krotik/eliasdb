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

import "testing"

func TestInfoQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointInfoQuery

	// No special testing here - the correctness of returned values is tested
	// elsewhere

	st, _, res := sendTestRequest(queryURL, "GET", nil)
	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}
}
