/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package access contains access control code for webservers.

Users (subjects) get rights to resources (objects) via groups. A group is
a collection of access rights. Users are members of groups. Access is denied
unless explicitly granted.
*/
package access

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"devt.de/common/datautil"
	"devt.de/common/errorutil"
	"devt.de/common/stringutil"
)

/*
ACLTable is a management object which can be used to define and enforce users rights.
*/
type ACLTable interface {

	/*
		Close closes this table.
	*/
	Close() error

	/*
		GroupNames returns a list of all known groups.
	*/
	GroupNames() ([]string, error)

	/*
		UserNames returns a list of all known users.
	*/
	UserNames() ([]string, error)

	/*
		GroupsOfUser for user returns the list of groups for a specific user.
	*/
	GroupsOfUser(name string) ([]string, error)

	/*
		AddPermission adds a new resource permission.
	*/
	AddPermission(group, resource string, permission *Rights) error

	/*
	   Permissions returns all permissions of a  group.
	*/
	Permissions(group string) (map[string]string, error)

	/*
	   ClearPermissions removes all permissions of a group.
	*/
	ClearPermissions(group string) error

	/*
		IsPermitted checks if a user has a certain permission. If the
		permission is given it also returns the rule which granted permission.
	*/
	IsPermitted(user, resource string, request *Rights) (bool, string, error)

	/*
		AddGroup creates a new group.
	*/
	AddGroup(name string) error

	/*
		RemoveGroup removes a group.
	*/
	RemoveGroup(name string) error

	/*
		AddUserToGroup adds a user to a group.
	*/
	AddUserToGroup(name string, group string) error

	/*
		RemoveUserFromGroup removes a user from a group.
	*/
	RemoveUserFromGroup(name string, group string) error

	/*
		GetConfig returns a data structure which contains the whole config of this
		ACLTable. The data structure can be easily converted into JSON.
	*/
	GetConfig() (map[string]interface{}, error)

	/*
		String returns a string representation of this ACL table.
	*/
	String() string
}

/*
Group is a collection of access rights.
*/
type Group struct {
	Name              string
	ResourceAccessAbs map[string]*Rights // Map from resource to access rights
	ResourceAccessPre map[string]*Rights // Map from resource prefix to access rights
}

/*
ClearResourceAccess removes all resource access rights from this group.
*/
func (g *Group) ClearResourceAccess() {
	g.ResourceAccessAbs = make(map[string]*Rights)
	g.ResourceAccessPre = make(map[string]*Rights)
}

/*
AddResourceAccess adds a new resource access right. A * as the resource
string suffix will grant access to all resources which start with the
resource string.
*/
func (g *Group) AddResourceAccess(res string, r *Rights) error {

	if strings.HasSuffix(res, "*") {
		pre := res[:len(res)-1]
		if _, ok := g.ResourceAccessPre[pre]; ok {
			return fmt.Errorf("Resource access wildcard for %v registered twice", res)
		}
		g.ResourceAccessPre[pre] = r
	} else {
		if _, ok := g.ResourceAccessAbs[res]; ok {
			return fmt.Errorf("Resource access for %v registered twice", res)
		}
		g.ResourceAccessAbs[res] = r
	}

	return nil
}

/*
IsPermitted checks if this group has access to a certain resource. Returns
also the rule which gives permission.
*/
func (g *Group) IsPermitted(resource string, request *Rights) (bool, string) {

	// First check direct match

	if r, ok := g.ResourceAccessAbs[resource]; ok {
		if r.IsAllowed(request) {
			return true, fmt.Sprintf("Group %v has specific access to %v with %v", g.Name, resource, r.String())
		}
	}

	// Go through prefixes

	for pre, r := range g.ResourceAccessPre {
		if strings.HasPrefix(resource, pre) && r.IsAllowed(request) {
			return true, fmt.Sprintf("Group %v has general access to %v with %v", g.Name, pre, r.String())
		}
	}

	return false, ""
}

/*
String returns the access rights of this group as a string table.
*/
func (g *Group) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("Group: %v\n====\n", g.Name))

	// Find out the longest name

	longest := 0
	for n := range g.ResourceAccessAbs {
		if len(n) > longest {
			longest = len(n)
		}
	}
	for n := range g.ResourceAccessPre {
		if len(n) > longest {
			longest = len(n)
		}
	}

	addResourceMap := func(m map[string]*Rights) {

		res := make([]string, 0, len(m))

		for n := range m {
			res = append(res, n)
		}

		sort.Strings(res)

		for _, n := range res {
			buf.WriteString(fmt.Sprintf("%-"+fmt.Sprint(longest)+"v : %v\n",
				n, m[n]))
		}
	}

	addResourceMap(g.ResourceAccessAbs)
	addResourceMap(g.ResourceAccessPre)

	return buf.String()
}

/*
Rights is an atomic permission of access.
*/
type Rights struct {
	Create bool // Create requests can be processed
	Read   bool // Read requests can be processed
	Update bool // Update requests can be processed
	Delete bool // Delete requests can be processed
}

/*
RightsFromString creates a new Rights object from a given rights string. A rights
string defines the access rights (c)reate, (r)ead, (u)pdate and (d)elete. Missing
rights are defined with a '-' sign. For example: read-only access would be '-r--',
full access would be 'crud'. REST APIs typically associate request types with these
rights: (c) POST, (r) GET, (u) PATCH, (d) DELETE.
*/
func RightsFromString(rights string) (*Rights, error) {
	var ret *Rights
	var c, r, u, d bool
	var err error

	if len(rights) != 4 {
		return nil, fmt.Errorf("Rigths string must be 4 characters")
	}

	rights = strings.ToLower(rights)

	parseChar := func(got byte, positive, desc string) (bool, error) {
		if string(got) == positive {
			return true, nil
		} else if string(got) == "-" {
			return false, nil
		}

		return false, fmt.Errorf("%v permission in rights string must be either '%v' or '-'", desc, positive)
	}

	if c, err = parseChar(rights[0], "c", "Create"); err == nil {
		if r, err = parseChar(rights[1], "r", "Read"); err == nil {
			if u, err = parseChar(rights[2], "u", "Update"); err == nil {
				d, err = parseChar(rights[3], "d", "Delete")
			}
		}
	}

	if err == nil {
		ret = &Rights{Create: c, Read: r, Update: u, Delete: d}
	}

	return ret, err
}

/*
IsAllowed checks if a given set of access requests is allowed by this set
of access permissions.
*/
func (r *Rights) IsAllowed(request *Rights) bool {

	if request.Create && request.Create != r.Create {
		return false
	} else if request.Read && request.Read != r.Read {
		return false
	} else if request.Update && request.Update != r.Update {
		return false
	} else if request.Delete && request.Delete != r.Delete {
		return false
	}

	return true
}

/*
String returns a string representation of this rights atom.
*/
func (r *Rights) String() string {
	ret := []string{"-", "-", "-", "-"}

	if r.Create {
		ret[0] = "C"
	}
	if r.Read {
		ret[1] = "R"
	}
	if r.Update {
		ret[2] = "U"
	}
	if r.Delete {
		ret[3] = "D"
	}

	return strings.Join(ret, "")
}

/*
MemoryACLTable is the main ACL table implementation. It stores permission
and group information in memory.
*/
type MemoryACLTable struct {
	PermissionCache *datautil.MapCache           // Cache for permission checks
	Users           map[string]map[string]*Group // Mapping from users to groups
	Groups          map[string]*Group            // Table of groups
}

/*
NewMemoryACLTable returns a new empty basic ACL table.
*/
func NewMemoryACLTable() ACLTable {
	return &MemoryACLTable{datautil.NewMapCache(300, 0),
		make(map[string]map[string]*Group), make(map[string]*Group)}
}

/*
NewMemoryACLTableFromConfig builds an ACL table from a given data structure which
was previously produced by GetConfig.
*/
func NewMemoryACLTableFromConfig(config map[string]interface{}) (ACLTable, error) {

	// Normalise the config object to avoid fighting the type system

	v, err := json.Marshal(config)

	if err == nil {

		err = json.Unmarshal(v, &config)

		if err == nil {

			usersData, ok := config["users"]
			if !ok {
				return nil, fmt.Errorf("Entry 'users' is missing from ACL table config")
			}

			users, ok := usersData.(map[string]interface{})

			if !ok {
				return nil, fmt.Errorf("Entry 'users' is not of the expected format")
			}

			groupsData, ok := config["groups"]

			if !ok {
				return nil, fmt.Errorf("Entry 'groups' is missing from ACL table config")
			}

			groups, ok := groupsData.(map[string]interface{})

			if !ok {
				return nil, fmt.Errorf("Entry 'groups' is not of the expected format")
			}

			tab := NewMemoryACLTable()

			// Create groups and their permissions to resources

			for g, res := range groups {
				err = tab.AddGroup(g)

				if _, ok := res.(map[string]interface{}); !ok {
					return nil, fmt.Errorf("Entries in 'groups' are not of the expected format")
				}

				for r, p := range res.(map[string]interface{}) {
					var rights *Rights

					rights, err = RightsFromString(fmt.Sprint(p))

					if err == nil {
						err = tab.AddPermission(g, r, rights)
					}

					if err != nil {
						err = fmt.Errorf("Error adding resource %v of group %v: %v",
							r, g, err.Error())
						break
					}
				}

				if err != nil {
					break
				}
			}

			// Add users to groups

			if err == nil {

				for u, gs := range users {

					if _, ok := gs.([]interface{}); !ok {
						return nil, fmt.Errorf("Entries in 'users' are not of the expected format")
					}

					for _, g := range gs.([]interface{}) {

						if err = tab.AddUserToGroup(u, fmt.Sprint(g)); err != nil {
							err = fmt.Errorf("Error adding user %v to group %v: %v",
								u, g, err.Error())
							break
						}
					}

					if err != nil {
						break
					}
				}
			}

			if err == nil {
				return tab, nil
			}
		}
	}

	return nil, err
}

/*
Close closes this table.
*/
func (t *MemoryACLTable) Close() error {

	// Nothing to do for the memory ACL table.

	return nil
}

/*
GroupNames returns a list of all known groups.
*/
func (t *MemoryACLTable) GroupNames() ([]string, error) {
	var ret []string

	if t != nil {
		for n := range t.Groups {
			ret = append(ret, n)
		}

		sort.Strings(ret)
	}

	return ret, nil
}

/*
UserNames returns a list of all known users.
*/
func (t *MemoryACLTable) UserNames() ([]string, error) {
	var ret []string

	if t != nil {

		for n := range t.Users {
			ret = append(ret, n)
		}

		sort.Strings(ret)
	}

	return ret, nil
}

/*
GroupsOfUser for user returns the list of groups for a specific user.
*/
func (t *MemoryACLTable) GroupsOfUser(name string) ([]string, error) {
	var ret []string
	var err error

	if ug, ok := t.Users[name]; ok {

		for n := range ug {
			ret = append(ret, n)
		}
		sort.Strings(ret)

	} else {

		err = fmt.Errorf("Unknown user: %v", name)
	}

	return ret, err
}

/*
AddPermission adds a new resource permission.
*/
func (t *MemoryACLTable) AddPermission(group, resource string, permission *Rights) error {

	g, ok := t.Groups[group]
	if !ok {
		return fmt.Errorf("Group %v does not exist", group)
	}

	t.invalidatePermCache()

	return g.AddResourceAccess(resource, permission)
}

/*
Permissions returns all permissions of a  group.
*/
func (t *MemoryACLTable) Permissions(group string) (map[string]string, error) {

	if _, ok := t.Groups[group]; !ok {
		return nil, fmt.Errorf("Group %v does not exist", group)
	}

	c, _ := t.GetConfig()

	return c["groups"].(map[string]map[string]string)[group], nil
}

/*
ClearPermissions removes all permissions of a group.
*/
func (t *MemoryACLTable) ClearPermissions(group string) error {

	g, ok := t.Groups[group]
	if !ok {
		return fmt.Errorf("Group %v does not exist", group)
	}

	t.invalidatePermCache()

	g.ClearResourceAccess()

	return nil
}

/*
invalidatePermCache invalidates the permission cache. This should be called before
any write operation.
*/
func (t *MemoryACLTable) invalidatePermCache() {
	t.PermissionCache.Clear()
}

/*
IsPermitted checks if a user has a certain permission. If the
permission is given it also returns the rule which granted permission.
*/
func (t *MemoryACLTable) IsPermitted(user, resource string, request *Rights) (bool, string, error) {
	var res bool
	var err error
	var reason string

	// Check from permission cache

	if res, ok := t.PermissionCache.Get(fmt.Sprint([]string{user, resource, request.String()})); ok {
		return res.(bool), "Permission was stored in the cache", err
	}

	if ug, ok := t.Users[user]; ok {

		// Check if one of the user's groups is permitted to access the resource

		for _, g := range ug {
			if ok, okReason := g.IsPermitted(resource, request); ok {
				res = true
				reason = okReason
				break
			}
		}

		// Store the result in the permission cache

		t.PermissionCache.Put(fmt.Sprint([]string{user, resource, request.String()}), res)

	} else {

		err = fmt.Errorf("Unknown user: %v", user)
	}

	return res, reason, err
}

/*
AddGroup creates a new group.
*/
func (t *MemoryACLTable) AddGroup(name string) error {

	if _, ok := t.Groups[name]; ok {
		return fmt.Errorf("Group %v added twice", name)
	}

	t.invalidatePermCache()

	g := &Group{name, make(map[string]*Rights), make(map[string]*Rights)}

	t.Groups[g.Name] = g

	return nil
}

/*
RemoveGroup removes a group.
*/
func (t *MemoryACLTable) RemoveGroup(name string) error {

	if _, ok := t.Groups[name]; !ok {
		return fmt.Errorf("Group %v does not exist", name)
	}

	t.invalidatePermCache()

	delete(t.Groups, name)

	for gname, gm := range t.Users {
		delete(gm, name)

		if len(gm) == 0 {

			// Users without any groups are removed

			delete(t.Users, gname)
		}
	}

	return nil
}

/*
AddUserToGroup adds a user to a group.
*/
func (t *MemoryACLTable) AddUserToGroup(name string, group string) error {

	g, ok := t.Groups[group]
	if !ok {
		return fmt.Errorf("Group %v does not exist", group)
	}

	t.invalidatePermCache()

	if l, ok := t.Users[name]; ok {
		if _, ok := l[group]; !ok {
			l[group] = g
		}
	} else {
		t.Users[name] = map[string]*Group{
			group: g,
		}
	}

	return nil
}

/*
RemoveUserFromGroup removes a user from a group.
*/
func (t *MemoryACLTable) RemoveUserFromGroup(name string, group string) error {
	var err error

	u, ok := t.Users[name]
	if !ok {
		return fmt.Errorf("User %v does not exist", name)
	}

	t.invalidatePermCache()

	if _, ok := u[group]; ok {
		delete(u, group)
	} else {
		err = fmt.Errorf("User %v is not in group %v", name, group)
	}

	return err
}

/*
String returns a string representation of this ACL table.
*/
func (t *MemoryACLTable) String() string {
	var buf bytes.Buffer

	buf.WriteString("ACLTable\n")
	buf.WriteString("========\n")

	buf.WriteString("Users:\n")

	usernames, _ := t.UserNames()
	for _, u := range usernames {
		g, err := t.GroupsOfUser(u)
		errorutil.AssertOk(err)
		buf.WriteString(fmt.Sprintf("%v : %v\n", u, strings.Join(g, ", ")))
	}

	groupnames, _ := t.GroupNames()
	for _, g := range groupnames {
		buf.WriteString("\n")
		buf.WriteString(t.Groups[g].String())
	}

	return buf.String()
}

/*
GetConfig returns a data structure which contains the whole config of this
ACLTable. The data structure can be easily converted into JSON.
*/
func (t *MemoryACLTable) GetConfig() (map[string]interface{}, error) {
	data := make(map[string]interface{})

	users := make(map[string][]string)
	data["users"] = users

	usernames, _ := t.UserNames()
	for _, u := range usernames {
		g, err := t.GroupsOfUser(u)
		errorutil.AssertOk(err)
		users[u] = g
	}

	groups := make(map[string]map[string]string)
	data["groups"] = groups

	groupnames, _ := t.GroupNames()
	for _, g := range groupnames {
		group := t.Groups[g]

		resources := make(map[string]string)
		groups[g] = resources

		for res, rights := range group.ResourceAccessAbs {
			resources[res] = rights.String()
		}
		for res, rights := range group.ResourceAccessPre {
			resources[res+"*"] = rights.String()
		}
	}

	return data, nil
}

/*
PersistedACLTableErrRetries is the number of times the code will try to
read the disk configuration before overwriting it with the current
(working) configuration. Set to -1 if it should never attempt to overwrite.
*/
var PersistedACLTableErrRetries = 10

/*
watchSleep is the sleep which is used by the watch thread
*/
var watchSleep = time.Sleep

/*
Defined error codes for PersistedACLTable
*/
var (
	ErrClosed = errors.New("ACL table was closed")
)

/*
PersistedACLTable is an ACL table whose state is persisted in a file and
in memory. The table in memory and the file on disk are kept automatically
in sync. This object is thread-safe. A persistent synchronization error between
file and memory table will lock this object down.
*/
type PersistedACLTable struct {
	table     ACLTable      // Internal in memory ACL Table
	tableLock *sync.RWMutex // Lock for ACL table
	interval  time.Duration // Interval with which the file should be watched
	filename  string        // File which stores the ACL table
	SyncError error         // Synchronization errors
	shutdown  chan bool     // Signal channel for thread shutdown
}

/*
NewPersistedACLTable returns a new file-persisted ACL table.
*/
func NewPersistedACLTable(filename string, interval time.Duration) (ACLTable, error) {
	var ret *PersistedACLTable

	ptable := &PersistedACLTable{nil, &sync.RWMutex{}, interval, filename, nil, nil}

	err := ptable.start()

	if err == nil {
		ret = ptable
	}

	return ret, err
}

/*
start kicks off the file watcher background thread.
*/
func (t *PersistedACLTable) start() error {

	// Sync from file - if the file exists. No need to hold a lock since
	// we are in the startup

	err := t.sync(true)

	if err == nil {

		// Kick off watcher

		t.shutdown = make(chan bool)

		go t.watch()
	}

	return err
}

/*
watch is the internal file watch goroutine function.
*/
func (t *PersistedACLTable) watch() {
	err := t.SyncError
	errCnt := 0

	defer func() {
		t.shutdown <- true
	}()

	for t.SyncError != ErrClosed {

		// Wakeup every interval

		watchSleep(t.interval)

		// Run the sync

		t.tableLock.Lock()

		// Sync from file

		if err = t.sync(true); err != nil && t.SyncError != ErrClosed {

			// Increase the error count

			err = fmt.Errorf("Could not sync ACL table config from disk: %v",
				err.Error())

			errCnt++

		} else {

			// Reset the error count

			errCnt = 0
		}

		// Update the sync error

		if t.SyncError != ErrClosed {
			t.SyncError = err
		}

		if errCnt == PersistedACLTableErrRetries {

			// We can't read the disk configuration after
			// PersistedACLTableErrRetries attempts - try to overwrite
			// it with the working memory configuration

			t.sync(false)
		}

		t.tableLock.Unlock()
	}
}

/*
Close closes this table.
*/
func (t *PersistedACLTable) Close() error {
	var err error

	t.tableLock.Lock()

	if t.SyncError != nil {

		// Preserve any old error

		err = t.SyncError
	}

	// Set the table into the closed state

	t.SyncError = ErrClosed

	t.tableLock.Unlock()

	// Wait for watcher shutdown if it was started

	if t.shutdown != nil {
		<-t.shutdown
		t.shutdown = nil
	}

	return err
}

/*
Attempt to synchronize the memory table with the file. Depending on the
checkFile flag either the file (true) or the memory table (false) is
regarded as up-to-date.

It is assumed that the tableLock (write) is held before calling this
function.

The table is in an undefined state if an error is returned.
*/
func (t *PersistedACLTable) sync(checkFile bool) error {
	var checksumFile, checksumMemory string

	stringMemoryTable := func() ([]byte, error) {
		tableconfig, _ := t.table.GetConfig()
		return json.MarshalIndent(tableconfig, "", "  ")
	}

	writeMemoryTable := func() error {
		res, err := stringMemoryTable()

		if err == nil {
			err = ioutil.WriteFile(t.filename, res, 0666)
		}

		return err
	}

	readMemoryTable := func() (map[string]interface{}, error) {
		var conf map[string]interface{}

		res, err := ioutil.ReadFile(t.filename)

		if err == nil {
			err = json.Unmarshal(stringutil.StripCStyleComments(res), &conf)
		}

		return conf, err
	}

	// Check if the file can be opened

	file, err := os.OpenFile(t.filename, os.O_RDONLY, 0660)

	if err != nil {

		if os.IsNotExist(err) {

			// Just ignore not found errors

			err = nil
		}

		// File does not exist - no checksum

		checksumFile = ""

	} else {

		hashFactory := sha256.New()

		if _, err = io.Copy(hashFactory, file); err == nil {

			// Create the checksum of the present file

			checksumFile = fmt.Sprintf("%x", hashFactory.Sum(nil))
		}

		file.Close()
	}

	if err == nil {

		// At this point we know everything about the file now check
		// the memory table

		if t.table != nil {
			var mtString []byte

			if mtString, err = stringMemoryTable(); err == nil {
				hashFactory := sha256.New()

				hashFactory.Write(mtString)

				checksumMemory = fmt.Sprintf("%x", hashFactory.Sum(nil))
			}

		} else {

			checksumMemory = ""
		}
	}

	if err == nil {

		// At this point we also know everything about the memory table

		if checkFile {

			// File is up-to-date - we should build the memory table

			if checksumFile == "" {

				// No file on disk just create an empty table and write it

				t.table = NewMemoryACLTable()
				err = writeMemoryTable()

			} else if checksumFile != checksumMemory {
				var conf map[string]interface{}

				if conf, err = readMemoryTable(); err == nil {

					t.table, err = NewMemoryACLTableFromConfig(conf)
				}
			}

		} else {

			// Memory is up-to-date - we should write a new file

			if checksumMemory == "" {

				// No data in memory just create an empty table and write it

				t.table = NewMemoryACLTable()
				err = writeMemoryTable()

			} else if checksumFile != checksumMemory {

				err = writeMemoryTable()
			}
		}
	}

	return err
}

/*
GroupNames returns a list of all known groups.
*/
func (t *PersistedACLTable) GroupNames() ([]string, error) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	if t.SyncError != nil {
		return nil, t.SyncError
	}

	return t.table.GroupNames()
}

/*
UserNames returns a list of all known users.
*/
func (t *PersistedACLTable) UserNames() ([]string, error) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	if t.SyncError != nil {
		return nil, t.SyncError
	}

	return t.table.UserNames()
}

/*
GroupsOfUser for user returns the list of groups for a specific user.
*/
func (t *PersistedACLTable) GroupsOfUser(name string) ([]string, error) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	if t.SyncError != nil {
		return nil, t.SyncError
	}

	return t.table.GroupsOfUser(name)
}

/*
AddPermission adds a new resource permission.
*/
func (t *PersistedACLTable) AddPermission(group, resource string, permission *Rights) error {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	if t.SyncError != nil {
		return t.SyncError
	}

	err := t.table.AddPermission(group, resource, permission)

	if err == nil {

		// Sync change to disk

		err = t.sync(false)
	}

	return err
}

/*
Permissions returns all permissions of a group.
*/
func (t *PersistedACLTable) Permissions(group string) (map[string]string, error) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	if t.SyncError != nil {
		return nil, t.SyncError
	}

	return t.table.Permissions(group)
}

/*
ClearPermissions removes all permissions of a group.
*/
func (t *PersistedACLTable) ClearPermissions(group string) error {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	if t.SyncError != nil {
		return t.SyncError
	}

	t.table.ClearPermissions(group)

	return t.sync(false)
}

/*
IsPermitted checks if a user has a certain permission. If the
permission is given it also returns the rule which granted permission.
*/
func (t *PersistedACLTable) IsPermitted(user, resource string, request *Rights) (bool, string, error) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	if t.SyncError != nil {
		return false, "", t.SyncError
	}

	return t.table.IsPermitted(user, resource, request)
}

/*
AddGroup creates a new group.
*/
func (t *PersistedACLTable) AddGroup(name string) error {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	if t.SyncError != nil {
		return t.SyncError
	}

	err := t.table.AddGroup(name)

	if err == nil {

		// Sync change to disk

		err = t.sync(false)
	}

	return err
}

/*
RemoveGroup removes a group.
*/
func (t *PersistedACLTable) RemoveGroup(name string) error {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	if t.SyncError != nil {
		return t.SyncError
	}

	err := t.table.RemoveGroup(name)

	if err == nil {

		// Sync change to disk

		err = t.sync(false)
	}

	return err
}

/*
AddUserToGroup adds a user to a group.
*/
func (t *PersistedACLTable) AddUserToGroup(name string, group string) error {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	if t.SyncError != nil {
		return t.SyncError
	}

	err := t.table.AddUserToGroup(name, group)

	if err == nil {

		// Sync change to disk

		err = t.sync(false)
	}

	return err
}

/*
RemoveUserFromGroup removes a user from a group.
*/
func (t *PersistedACLTable) RemoveUserFromGroup(name string, group string) error {
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	if t.SyncError != nil {
		return t.SyncError
	}

	err := t.table.RemoveUserFromGroup(name, group)

	if err == nil {

		// Sync change to disk

		err = t.sync(false)
	}

	return err
}

/*
GetConfig returns a data structure which contains the whole config of this
ACLTable. The data structure can be easily converted into JSON.
*/
func (t *PersistedACLTable) GetConfig() (map[string]interface{}, error) {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	if t.SyncError != nil {
		return nil, t.SyncError
	}

	return t.table.GetConfig()
}

/*
String returns a string representation of this ACL table.
*/
func (t *PersistedACLTable) String() string {
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()

	return t.table.String()
}
