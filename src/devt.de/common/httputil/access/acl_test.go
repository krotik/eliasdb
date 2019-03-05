package access

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"devt.de/common/fileutil"
)

func TestPersistedACLTable(t *testing.T) {
	testACLFile := "persist_tester.acl"
	defer func() {
		os.Remove(testACLFile)
	}()

	// Test the most basic start and stop

	pt, err := NewPersistedACLTable(testACLFile, time.Millisecond)
	if err != nil {
		t.Error(err)
		return
	}

	if _, _, err := pt.IsPermitted("", "", &Rights{}); err == nil || err.Error() != "Unknown user: " {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.Close(); err != nil {
		t.Error(err)
		return
	}

	// Check that we get an error back when trying to close a thing twice or we try any other operation

	if _, _, err := pt.IsPermitted("", "", &Rights{}); err == nil || err.Error() != "ACL table was closed" {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.Close(); err == nil || err.Error() != "ACL table was closed" {
		t.Error("Unexpected result:", err)
		return
	}

	// Create communication channel which can be used to trigger the watch thread

	watchToggle := make(chan bool)
	watchSleep = func(t time.Duration) {
		<-watchToggle
	}

	// Now test again but with some data

	pt, err = NewPersistedACLTable(testACLFile, time.Millisecond)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(50 * time.Millisecond)

	if pt.String() != `
ACLTable
========
Users:
`[1:] {
		t.Error("Unexpected result:", pt.String())
		return
	}

	// Add the fi

	if err := pt.AddGroup("foo"); err != nil {
		t.Error(err)
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `
{
  "groups": {
    "foo": {}
  },
  "users": {}
}`[1:] {
		t.Error("Unexpected result:", string(res))
		return
	}

	if err := ioutil.WriteFile(testACLFile, []byte(`
{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D"
    }
  },
  "users": {
    "hans": [
      "public"
    ]
  }
}`), 0666); err != nil {
		t.Error(err)
		return
	}

	watchToggle <- true

	time.Sleep(50 * time.Millisecond)

	// Check the new configuration has been loaded from disk

	if pt.String() != `
ACLTable
========
Users:
hans : public

Group: bar
====
/bla     : CRUD
/blatest : -R-D

Group: public
====
/special : -R-D
/test    : -R-D
`[1:] {
		t.Error("Unexpected result:", pt.String())
		return
	}

	// Produce some faulty disk configuration and see that it is rewritten
	// after PersistedACLTableErrRetries

	PersistedACLTableErrRetries = 2

	if err := ioutil.WriteFile(testACLFile, []byte(`
{
  "groups": {
    "bar": {
`), 0666); err != nil {
		t.Error(err)
		return
	}

	watchToggle <- true
	time.Sleep(time.Millisecond)
	watchToggle <- true
	time.Sleep(time.Millisecond)

	if err := pt.(*PersistedACLTable).SyncError; err == nil ||
		err.Error() != "Could not sync ACL table config from disk: unexpected end of JSON input" {
		t.Error("Unexpected result:", err)
		return
	}

	watchToggle <- true
	time.Sleep(50 * time.Millisecond)

	if err := pt.(*PersistedACLTable).SyncError; err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D"
    }
  },
  "users": {
    "hans": [
      "public"
    ]
  }
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	if pt.String() != `
ACLTable
========
Users:
hans : public

Group: bar
====
/bla     : CRUD
/blatest : -R-D

Group: public
====
/special : -R-D
/test    : -R-D
`[1:] {
		t.Error("Unexpected result:", pt.String())
		return
	}

	// Test the various access functions

	if res, err := pt.GroupNames(); fmt.Sprint(res) != "[bar public]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := pt.UserNames(); fmt.Sprint(res) != "[hans]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := pt.GroupsOfUser("hans"); fmt.Sprint(res) != "[public]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if err := pt.AddPermission("public", "woo", &Rights{}); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D",
      "woo": "----"
    }
  },
  "users": {
    "hans": [
      "public"
    ]
  }
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	if err := pt.AddUserToGroup("hans", "bar"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D",
      "woo": "----"
    }
  },
  "users": {
    "hans": [
      "bar",
      "public"
    ]
  }
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	conf, err := pt.GetConfig()
	if err != nil {
		t.Error(err)
		return
	}

	res, err := json.MarshalIndent(conf, "", "  ")

	if err != nil || string(res) != `
{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D",
      "woo": "----"
    }
  },
  "users": {
    "hans": [
      "bar",
      "public"
    ]
  }
}`[1:] {
		t.Error("Unexpected result:", string(res), err)
		return
	}

	if err := pt.RemoveUserFromGroup("hans", "bar"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D",
      "woo": "----"
    }
  },
  "users": {
    "hans": [
      "public"
    ]
  }
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	if err := pt.RemoveGroup("public"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    }
  },
  "users": {}
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	if err := pt.ClearPermissions("bar"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := pt.Permissions("bar"); fmt.Sprint(res) != "map[]" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Test that a sync failure blocks the whole object

	testerror := fmt.Errorf("Testerror")
	pt.(*PersistedACLTable).SyncError = testerror

	if _, err := pt.GroupNames(); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if _, err := pt.UserNames(); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if _, err := pt.GroupsOfUser(""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.AddPermission("", "", nil); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if _, err := pt.Permissions(""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.ClearPermissions(""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.AddGroup(""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.RemoveGroup(""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.AddUserToGroup("", ""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if err := pt.RemoveUserFromGroup("", ""); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	if _, err := pt.GetConfig(); err != testerror {
		t.Error("Unexpected result:", err)
		return
	}

	// Toggle the watch routine and close

	pt.(*PersistedACLTable).SyncError = ErrClosed
	watchToggle <- true

	err = pt.Close()

	// Error is preserved by by Close since it was set before it was called

	if err != ErrClosed {
		t.Error(err)
	}

}

func TestPersistedACLTableSync(t *testing.T) {
	testACLFile := "sycn_tester.acl"
	defer func() {
		os.Remove(testACLFile)
	}()

	pt := &PersistedACLTable{}

	pt.filename = testACLFile

	// We start with no file and no table

	if ok, _ := fileutil.PathExists(testACLFile); ok {
		t.Error("Unexpected result:", ok)
		return
	}

	// This should create an empty table in memory and on disk

	if err := pt.sync(true); err != nil {
		t.Error(err)
		return
	}

	if ok, _ := fileutil.PathExists(testACLFile); !ok {
		t.Error("Unexpected result:", ok)
		return
	}

	if pt.table.String() != `
ACLTable
========
Users:
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {},
  "users": {}
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	os.Remove(testACLFile)

	if ok, _ := fileutil.PathExists(testACLFile); ok {
		t.Error("Unexpected result:", ok)
		return
	}

	pt.table = nil

	// Now do the same excercise again but with syncing from memory

	if err := pt.sync(false); err != nil {
		t.Error(err)
		return
	}

	if ok, _ := fileutil.PathExists(testACLFile); !ok {
		t.Error("Unexpected result:", ok)
		return
	}

	if pt.table.String() != `
ACLTable
========
Users:
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `{
  "groups": {},
  "users": {}
}` {
		t.Error("Unexpected result:", string(res))
		return
	}

	// Fill up the memory but sync from disk

	pt.table.AddGroup("foo")

	if pt.table.String() != `
ACLTable
========
Users:

Group: foo
====
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}

	if res, err := pt.table.GetConfig(); fmt.Sprint(res["groups"]) != `map[foo:map[]]` || err != nil {
		t.Error("Unexpected result:", res["groups"], err)
		return
	}

	if err := pt.sync(true); err != nil {
		t.Error(err)
		return
	}

	if pt.table.String() != `
ACLTable
========
Users:
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}

	// Fill up the memory and sync to disk

	pt.table.AddGroup("foo")

	if pt.table.String() != `
ACLTable
========
Users:

Group: foo
====
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}

	if err := pt.sync(false); err != nil {
		t.Error(err)
		return
	}

	if pt.table.String() != `
ACLTable
========
Users:

Group: foo
====
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}

	if res, _ := ioutil.ReadFile(testACLFile); string(res) != `
{
  "groups": {
    "foo": {}
  },
  "users": {}
}`[1:] {
		t.Error("Unexpected result:", string(res))
		return
	}

	// Write some rules to disk and load them into memory

	if err := ioutil.WriteFile(testACLFile, []byte(`
{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D"
    }
  },
  "users": {
    "hans": [
      "public"
    ]
  }
}`), 0666); err != nil {
		t.Error(err)
		return
	}

	if err := pt.sync(true); err != nil {
		t.Error(err)
		return
	}

	if pt.table.String() != `
ACLTable
========
Users:
hans : public

Group: bar
====
/bla     : CRUD
/blatest : -R-D

Group: public
====
/special : -R-D
/test    : -R-D
`[1:] {
		t.Error("Unexpected result:", pt.table.String())
		return
	}
}

func TestRigths(t *testing.T) {

	r1 := &Rights{}
	r1.Create = true
	r1.Delete = true

	r2 := &Rights{
		Create: true,
		Delete: true,
		Update: true,
	}

	if res := r2.String(); res != "C-UD" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := r2.IsAllowed(r1); !res {
		t.Error("Unexpected result:", res)
		return
	}

	if res := r1.IsAllowed(r2); res {
		t.Error("Unexpected result:", res)
		return
	}

	r1 = &Rights{}

	if res := r1.IsAllowed(&Rights{Read: true}); res {
		t.Error("Unexpected result:", res)
		return
	}

	if res := r1.IsAllowed(&Rights{Create: true}); res {
		t.Error("Unexpected result:", res)
		return
	}

	if res := r1.IsAllowed(&Rights{Update: true}); res {
		t.Error("Unexpected result:", res)
		return
	}

	if res := r1.IsAllowed(&Rights{Delete: true}); res {
		t.Error("Unexpected result:", res)
		return
	}

	if res, err := RightsFromString("C-U"); res != nil || err == nil || err.Error() != "Rigths string must be 4 characters" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := RightsFromString("C0UD"); res != nil || err == nil || err.Error() != "Read permission in rights string must be either 'r' or '-'" {
		t.Error("Unexpected result:", res, err)
		return
	}
}

func TestMemoryACLTable(t *testing.T) {

	tab := NewMemoryACLTable()

	tab.Close() // Test NOP

	tableconfig, _ := tab.GetConfig()
	if res, err := json.MarshalIndent(tableconfig, "", "  "); err != nil || string(res) != `
{
  "groups": {},
  "users": {}
}`[1:] {
		t.Error("Unexpected result:", string(res), err)
		return
	}

	// Test empty table operations

	if res, _ := tab.GroupNames(); fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := tab.UserNames(); fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, err := tab.GroupsOfUser("hans"); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if err := tab.AddPermission("public", "", nil); err == nil || err.Error() != "Group public does not exist" {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.RemoveGroup("foo"); err == nil || err.Error() != "Group foo does not exist" {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.RemoveUserFromGroup("hans", "foo"); err == nil || err.Error() != "User hans does not exist" {
		t.Error("Unexpected result:", err)
		return
	}

	if _, _, err := tab.IsPermitted("hans", "", &Rights{}); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", err)
		return
	}

	// Manually fill up the table

	if err := tab.AddGroup("public"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := tab.Permissions("public"); fmt.Sprint(res) != "map[]" || err != nil {
		t.Error("Unexpected result: ", res, err)
		return
	}

	if res, err := tab.Permissions("public2"); err == nil || err.Error() != "Group public2 does not exist" {
		t.Error("Unexpected result: ", res, err)
		return
	}

	if err := tab.ClearPermissions("public2"); err == nil || err.Error() != "Group public2 does not exist" {
		t.Error("Unexpected result: ", err)
		return
	}

	if err := tab.AddGroup("public"); err == nil || err.Error() != "Group public added twice" {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := tab.GroupNames(); fmt.Sprint(res) != "[public]" {
		t.Error("Unexpected result:", res)
		return
	}

	if err := tab.AddGroup("bar"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.AddUserToGroup("hans", "foo"); err == nil || err.Error() != "Group foo does not exist" {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := tab.UserNames(); fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	if err := tab.AddUserToGroup("hans", "public"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.RemoveUserFromGroup("hans", "foo"); err == nil || err.Error() != "User hans is not in group foo" {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.AddUserToGroup("hans", "bar"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, _ := tab.UserNames(); fmt.Sprint(res) != "[hans]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/test", &Rights{}); err != nil || res {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := tab.GroupsOfUser("hans"); err != nil || fmt.Sprint(res) != "[bar public]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if err := tab.RemoveUserFromGroup("hans", "bar"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := tab.GroupsOfUser("hans"); err != nil || fmt.Sprint(res) != "[public]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Finally add some permissions

	if err := tab.AddPermission("public", "/special", &Rights{
		Read:   true,
		Delete: true,
	}); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := tab.Permissions("public"); fmt.Sprint(res) != "map[/special:-R-D]" || err != nil {
		t.Error("Unexpected result: ", res, err)
		return
	}

	if err := tab.ClearPermissions("public"); err != nil {
		t.Error("Unexpected result: ", err)
		return
	}

	if res, err := tab.Permissions("public"); fmt.Sprint(res) != "map[]" || err != nil {
		t.Error("Unexpected result: ", res, err)
		return
	}

	if err := tab.AddPermission("public", "/special", &Rights{
		Read:   true,
		Delete: true,
	}); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := tab.Permissions("public"); fmt.Sprint(res) != "map[/special:-R-D]" || err != nil {
		t.Error("Unexpected result: ", res, err)
		return
	}

	if err := tab.AddPermission("public", "/special", &Rights{}); err == nil || err.Error() != "Resource access for /special registered twice" {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.AddPermission("public", "/test*", &Rights{
		Read:   true,
		Delete: true,
	}); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.AddPermission("public", "/test*", &Rights{}); err == nil || err.Error() != "Resource access wildcard for /test* registered twice" {
		t.Error("Unexpected result:", err)
		return
	}

	// Check permissions

	if res, _, err := tab.IsPermitted("hans", "/test", &Rights{}); err != nil || !res {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/special", &Rights{}); err != nil || !res {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/special1", &Rights{}); err != nil || res {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Test no access in sub category

	if res, _, err := tab.IsPermitted("hans", "/test1", &Rights{}); err != nil || !res {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Test read access in sub category

	if res, _, err := tab.IsPermitted("hans", "/test1", &Rights{Read: true}); err != nil || !res {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/special", &Rights{Read: true}); err != nil || !res {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Test no update access

	if res, _, err := tab.IsPermitted("hans", "/test1", &Rights{Read: true, Update: true}); err != nil || res {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/special", &Rights{Read: true, Update: true}); err != nil || res {
		t.Error("Unexpected result:", res, err)
		return
	}

	// This should now be cached

	if res, _, err := tab.IsPermitted("hans", "/test1", &Rights{Read: true}); err != nil || !res {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/test1", &Rights{Read: true, Update: true}); err != nil || res {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Check a non-permitted access request

	if res, _, err := tab.IsPermitted("hans", "/tes", &Rights{}); err != nil || res {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Check cache contents

	if res := tab.(*MemoryACLTable).PermissionCache.String(); res != `
[hans /special ----]:true
[hans /special -R--]:true
[hans /special -RU-]:false
[hans /special1 ----]:false
[hans /tes ----]:false
[hans /test ----]:true
[hans /test1 ----]:true
[hans /test1 -R--]:true
[hans /test1 -RU-]:false
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	// Print current table

	if res := tab.String(); res != `
ACLTable
========
Users:
hans : public

Group: bar
====

Group: public
====
/special : -R-D
/test    : -R-D
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	if err := tab.AddPermission("bar", "/blatest*", &Rights{
		Read:   true,
		Delete: true,
	}); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := tab.AddPermission("bar", "/bla", &Rights{
		Read:   true,
		Create: true,
		Update: true,
		Delete: true,
	}); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res := tab.String(); res != `
ACLTable
========
Users:
hans : public

Group: bar
====
/bla     : CRUD
/blatest : -R-D

Group: public
====
/special : -R-D
/test    : -R-D
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	conf, err := tab.GetConfig()
	if err != nil {
		t.Error(err)
		return
	}

	res, err := json.MarshalIndent(conf, "", "  ")

	if err != nil || string(res) != `
{
  "groups": {
    "bar": {
      "/bla": "CRUD",
      "/blatest*": "-R-D"
    },
    "public": {
      "/special": "-R-D",
      "/test*": "-R-D"
    }
  },
  "users": {
    "hans": [
      "public"
    ]
  }
}`[1:] {
		t.Error("Unexpected result:", string(res), err)
		return
	}

	// Duplicate the table

	tab2, err := NewMemoryACLTableFromConfig(conf)
	if err != nil {
		t.Error(err)
		return
	}

	if res := tab2.String(); res != `
ACLTable
========
Users:
hans : public

Group: bar
====
/bla     : CRUD
/blatest : -R-D

Group: public
====
/special : -R-D
/test    : -R-D
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	// Destroy group public

	if err := tab.RemoveGroup("public"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := tab.GroupsOfUser("hans"); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", res, err)
		return
	}

	// All permissions should be gone now

	if res, _, err := tab.IsPermitted("hans", "/test1", &Rights{Read: true}); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _ := tab.GroupNames(); fmt.Sprint(res) != "[bar]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := tab.UserNames(); fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, err := tab.GroupsOfUser("hans"); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/test", &Rights{}); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, _, err := tab.IsPermitted("hans", "/tes", &Rights{}); err == nil || err.Error() != "Unknown user: hans" {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Test error cases

	tab2 = nil

	var nullTable *MemoryACLTable

	if res := nullTable.String(); res != `
ACLTable
========
Users:
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	tab2 = NewMemoryACLTable()

	if res := tab2.String(); res != `
ACLTable
========
Users:
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{})
	if tab != nil || err == nil || err.Error() != "Entry 'users' is missing from ACL table config" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users": "foo",
	})
	if tab != nil || err == nil || err.Error() != "Entry 'users' is not of the expected format" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users": map[string][]string{},
	})
	if tab != nil || err == nil || err.Error() != "Entry 'groups' is missing from ACL table config" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users":  map[string][]string{},
		"groups": "foo",
	})
	if tab != nil || err == nil || err.Error() != "Entry 'groups' is not of the expected format" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users": map[string][]string{},
		"groups": map[string]string{
			"foo": "bar",
		},
	})
	if tab != nil || err == nil || err.Error() != "Entries in 'groups' are not of the expected format" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users": map[string]string{
			"foo": "bar",
		},
		"groups": map[string]string{},
	})
	if tab != nil || err == nil || err.Error() != "Entries in 'users' are not of the expected format" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users": map[string][]string{},
		"groups": map[string]map[string]string{
			"foogroup": map[string]string{
				"/fooresource": "a---",
			},
		},
	})
	if tab != nil || err == nil ||
		err.Error() != "Error adding resource /fooresource of group foogroup: Create permission in rights string must be either 'c' or '-'" {
		t.Error("Unexpected results:", tab, err)
		return
	}

	tab, err = NewMemoryACLTableFromConfig(map[string]interface{}{
		"users": map[string][]string{
			"hans": []string{"bargroup"},
		},
		"groups": map[string]map[string]string{
			"foogroup": map[string]string{
				"/fooresource": "C---",
			},
		},
	})
	if tab != nil || err == nil ||
		err.Error() != "Error adding user hans to group bargroup: Group bargroup does not exist" {
		t.Error("Unexpected results:", tab, err)
		return
	}
}
