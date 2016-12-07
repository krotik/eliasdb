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
	"reflect"
	"testing"
)

type TokenTester struct {
	t              *testing.T
	invalidRequest map[RequestArgument]interface{}
}

func (t TokenTester) TestPing() {
	if err := server.Ping(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestAcquireLock() {
	if err := server.AcquireLock(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestReleaseLock() {
	if err := server.ReleaseLock(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestAddMember() {
	if err := server.AddMember(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestEjectMember() {
	if err := server.EjectMember(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestJoinCluster() {
	if err := server.JoinCluster(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestStateInfoRequest() {
	if err := server.StateInfoRequest(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestMemberInfoRequest() {
	if err := server.MemberInfoRequest(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestUpdateStateInfo() {
	if err := server.UpdateStateInfo(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func (t TokenTester) TestDataRequest() {
	if err := server.DataRequest(t.invalidRequest, nil); err.Error() != "Invalid member token" {
		t.t.Error("Unexpected result:", err)
		return
	}
}

func TestTokenCheck(t *testing.T) {

	// Check that all exposed functions check the giveen token

	mm := createCluster(1)[0]
	mm.Start()
	defer mm.Shutdown()

	request := make(map[RequestArgument]interface{})

	request[RequestTARGET] = "TestClusterMember-0"
	request[RequestTOKEN] = &MemberToken{"123", "123"}

	tester := &TokenTester{t, request}

	typ := reflect.TypeOf(server)
	testerVal := reflect.ValueOf(tester)

	for m := 0; m < typ.NumMethod(); m++ {
		methodType := typ.Method(m)
		mname := methodType.Name

		if mname == "checkToken" {

			// Exclude the checkToken function itself

			continue
		}

		method := testerVal.MethodByName("Test" + mname)

		if !method.IsValid() {
			t.Error("Method test for ", mname, "missing")
			continue
		}

		method.Call([]reflect.Value{})
	}
}
