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
	"testing"

	"devt.de/krotik/eliasdb/config"
)

func TestUsersCommands(t *testing.T) {
	var out bytes.Buffer
	var pass = "!El1as9845"

	ResetDB()
	credGiver.Reset()

	// Enable access control

	config.Config[config.EnableAccessControl] = true
	defer func() {
		config.Config[config.EnableAccessControl] = false
	}()

	c := NewConsole("http://localhost"+TESTPORT, &out, credGiver.GetCredentials,
		func() string { return pass },
		func(args []string, e *bytes.Buffer) error {
			return nil
		})

	credGiver.UserQueue = []string{"elias"}
	credGiver.PassQueue = []string{"elias"}

	if ok, err := c.Run("users"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
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

	if ok, err := c.Run("useradd"); ok || err == nil || err.Error() != "Please specify a username" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("useradd ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
User ml was created
`[1:] {
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
│ml       │             │
└─────────┴─────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("newpass"); ok || err == nil || err.Error() != "Please specify a username" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	pass = "!El1as9846"
	if ok, err := c.Run("newpass ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Password for user ml was changed
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("joingroup"); ok || err == nil || err.Error() != "Please specify a username and a group" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("joingroup ml public"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
User ml has joined group public
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("joingroup ml public"); ok || err == nil || err.Error() != "User ml is already member of group public" {
		t.Error(ok, err)
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
│ml       │public       │
└─────────┴─────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("groups"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌───────┬──────────┬────────────┐
│Group  │Path      │Permissions │
├───────┼──────────┼────────────┤
│admin  │/db/*     │CRUD        │
│public │/         │-R--        │
│       │/css/*    │-R--        │
│       │/db/*     │-R--        │
│       │/img/*    │-R--        │
│       │/js/*     │-R--        │
│       │/vendor/* │-R--        │
└───────┴──────────┴────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	// Creating special group

	out.Reset()

	if ok, err := c.Run("groupadd"); ok || err == nil || err.Error() != "Please specify a groupname" {
		t.Error(ok, err)
		return
	}

	if ok, err := c.Run("groupadd ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Group ml was created
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("groups"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌───────┬──────────┬────────────┐
│Group  │Path      │Permissions │
├───────┼──────────┼────────────┤
│admin  │/db/*     │CRUD        │
│ml     │          │            │
│public │/         │-R--        │
│       │/css/*    │-R--        │
│       │/db/*     │-R--        │
│       │/img/*    │-R--        │
│       │/js/*     │-R--        │
│       │/vendor/* │-R--        │
└───────┴──────────┴────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	// Test grant / revoke

	if ok, err := c.Run("grantperm"); ok || err == nil || err.Error() != "Please specify a permission, a resource path and a groupname" {
		t.Error(ok, err)
		return
	}

	if ok, err := c.Run("revokeperm"); ok || err == nil || err.Error() != "Please specify a resource path and a groupname" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("grantperm -r-- /styles/* ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Permission -r-- on /styles/* was granted to ml
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("grantperm -r-- /styles2/* ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Permission -r-- on /styles2/* was granted to ml
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("groups"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌───────┬───────────┬────────────┐
│Group  │Path       │Permissions │
├───────┼───────────┼────────────┤
│admin  │/db/*      │CRUD        │
│ml     │/styles/*  │-R--        │
│       │/styles2/* │-R--        │
│public │/          │-R--        │
│       │/css/*     │-R--        │
│       │/db/*      │-R--        │
│       │/img/*     │-R--        │
│       │/js/*      │-R--        │
│       │/vendor/*  │-R--        │
└───────┴───────────┴────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("revokeperm /styles2/* ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
All permissions on /styles2/* were revoked for ml
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("groups"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌───────┬──────────┬────────────┐
│Group  │Path      │Permissions │
├───────┼──────────┼────────────┤
│admin  │/db/*     │CRUD        │
│ml     │/styles/* │-R--        │
│public │/         │-R--        │
│       │/css/*    │-R--        │
│       │/db/*     │-R--        │
│       │/img/*    │-R--        │
│       │/js/*     │-R--        │
│       │/vendor/* │-R--        │
└───────┴──────────┴────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("groupdel"); ok || err == nil || err.Error() != "Please specify a groupname" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("groupdel ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Group ml was deleted
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("leavegroup"); ok || err == nil || err.Error() != "Please specify a username and a group" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("leavegroup ml foo"); ok || err == nil || err.Error() != "User ml is not in group foo" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("leavegroup ml public"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
User ml has left group public
`[1:] {
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
│ml       │             │
└─────────┴─────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("userdel"); ok || err == nil || err.Error() != "Please specify a username" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("userdel ml"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
User ml was deleted
`[1:] {
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
}
