/* 
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

/*
HTree implementation which uses a StorageManager. The HTree provides a persistent
hashtable. It is not possible to store nil values. Storing a nil value
is equivalent to removing a key.

The default tree has 4 levels each with 256 possible children. A hash code
for the tree has 32 bits = 4 levels * 8 bit.
*/
package hash

import (
	"fmt"
	"sync"

	"devt.de/eliasdb/storage"
)

/*
Maximum number of non-leaf levels in the tree (i.e. the complete tree has
a total of MAX_DEPTH+1 levels)
*/
const MAX_TREE_DEPTH = 3

/*
Significant bits per page level
*/
const PAGE_LEVEL_BITS = 8

/*
Max number of children per page - (stored in 8 bit)
*/
const MAX_PAGE_CHILDREN = 256

/*
Max number of elements a bucket can contain before it is converted into a page
except leaf buckets which grow indefinitely
*/
const MAX_BUCKET_ELEMENTS = 8

/*
HTree data structure
*/
type HTree struct {
	Root  *htreePage  //  Root page of the HTree
	mutex *sync.Mutex // Mutex to protect tree operations
}

/*
htreeNode data structure - this object models the
HTree storage structure on disk
*/
type htreeNode struct {
	tree *HTree                 // Reference to the HTree which owns this node (not persisted)
	loc  uint64                 // Storage location of this page (not persisted)
	sm   storage.StorageManager // StorageManager instance which stores the tree data (not persisted)

	Depth      byte          // Depth of this node
	Children   []uint64      // Storage locations of children (only used for pages)
	Keys       [][]byte      // Stored keys (only used for buckets)
	Values     []interface{} // Stored values (only used for buckets)
	BucketSize byte          // Bucket size (only used for buckets)
}

/*
Fetch a HTree node from the storage.
*/
func (n *htreeNode) fetchNode(loc uint64) (*htreeNode, error) {
	var node *htreeNode

	if obj, _ := n.sm.FetchCached(loc); obj == nil {
		var res htreeNode
		if err := n.sm.Fetch(loc, &res); err != nil {
			return nil, err
		}
		node = &res
	} else {
		node = obj.(*htreeNode)
	}

	return node, nil
}

/*
NewHTree creates a new HTree.
*/
func NewHTree(sm storage.StorageManager) (*HTree, error) {
	tree := &HTree{}

	// Protect tree creation

	cm := &sync.Mutex{}
	cm.Lock()
	defer cm.Unlock()

	tree.Root = newHTreePage(tree, 0)

	loc, err := sm.Insert(tree.Root.htreeNode)
	if err != nil {
		return nil, err
	}

	tree.Root.loc = loc
	tree.Root.sm = sm

	tree.mutex = &sync.Mutex{}

	return tree, nil
}

/*
LoadHTree fetches a HTree from storage
*/
func LoadHTree(sm storage.StorageManager, loc uint64) (*HTree, error) {
	var tree *HTree

	// Protect tree creation

	cm := &sync.Mutex{}
	cm.Lock()
	defer cm.Unlock()

	if obj, _ := sm.FetchCached(loc); obj == nil {
		var res htreeNode
		if err := sm.Fetch(loc, &res); err != nil {
			return nil, err
		}
		tree = &HTree{&htreePage{&res}, nil}
	} else {
		tree = &HTree{&htreePage{obj.(*htreeNode)}, nil}
	}

	tree.Root.loc = loc
	tree.Root.sm = sm

	tree.mutex = &sync.Mutex{}

	return tree, nil
}

/*
Location returns the HTree location on disk.
*/
func (t *HTree) Location() uint64 {
	return t.Root.loc
}

/*
Get gets a value for a given key.
*/
func (t *HTree) Get(key []byte) (interface{}, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	res, _, err := t.Root.Get(key)

	return res, err
}

/*
GetValueAndLocation returns the value and the storage location for a given key.
*/
func (t *HTree) GetValueAndLocation(key []byte) (interface{}, uint64, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	res, bucket, err := t.Root.Get(key)

	if bucket != nil {
		return res, bucket.loc, err
	} else {
		return res, 0, err
	}
}

/*
Exists checks if an element exists.
*/
func (t *HTree) Exists(key []byte) (bool, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.Root.Exists(key)
}

/*
Put adds or updates a new key / value pair.
*/
func (t *HTree) Put(key []byte, value interface{}) (interface{}, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.Root.Put(key, value)
}

/*
Remove removes a key / value pair.
*/
func (t *HTree) Remove(key []byte) (interface{}, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.Root.Remove(key)
}

/*
String returns a string representation of this tree.
*/
func (t *HTree) String() string {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return fmt.Sprintf("HTree: %v (%v)\n%v", t.Root.sm.Name(), t.Root.loc, t.Root.String())
}
