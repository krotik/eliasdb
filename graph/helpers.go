/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package graph

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strings"

	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/util"
	"devt.de/krotik/eliasdb/hash"
	"devt.de/krotik/eliasdb/storage"
)

// Helper functions for GraphManager
// =================================

/*
checkPartitionName checks if a given partition name is valid.
*/
func (gm *Manager) checkPartitionName(part string) error {
	if !stringutil.IsAlphaNumeric(part) {
		return &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("Partition name %v is not alphanumeric - can only contain [a-zA-Z0-9_]", part),
		}
	}

	return nil
}

/*
checkNode checks if a given node can be written to the datastore.
*/
func (gm *Manager) checkNode(node data.Node) error {
	return gm.checkItemGeneral(node, "Node")
}

/*
checkItemGeneral checks the general properties of a given graph item.
*/
func (gm *Manager) checkItemGeneral(node data.Node, name string) error {
	if node.Key() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: name + " is missing a key value"}
	}

	if node.Kind() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: name + " is missing a kind value"}
	}

	if !stringutil.IsAlphaNumeric(node.Kind()) {
		return &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("%v kind %v is not alphanumeric - can only contain [a-zA-Z0-9_]", name, node.Kind()),
		}
	}

	for attr := range node.Data() {
		if attr == "" {
			return &util.GraphError{Type: util.ErrInvalidData, Detail: name + " contains empty string attribute name"}
		}
	}

	return nil
}

/*
checkEdge checks if a given edge can be written to the datastore.
*/
func (gm *Manager) checkEdge(edge data.Edge) error {
	if err := gm.checkItemGeneral(edge, "Edge"); err != nil {
		return err
	}

	if edge.End1Key() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a key value for end1"}
	}

	if edge.End1Kind() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a kind value for end1"}
	}

	if edge.End1Role() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a role value for end1"}
	} else if !stringutil.IsAlphaNumeric(edge.End1Role()) {
		return &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("Edge role %v is not alphanumeric - can only contain [a-zA-Z0-9_]", edge.End1Role()),
		}
	}

	if _, ok := edge.Attr(data.EdgeEnd1Cascading).(bool); !ok {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a cascading value for end1"}
	}

	if edge.End2Key() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a key value for end2"}
	}

	if edge.End2Kind() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a kind value for end2"}
	}

	if edge.End2Role() == "" {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a role value for end2"}
	} else if !stringutil.IsAlphaNumeric(edge.End2Role()) {
		return &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("Edge role %v is not alphanumeric - can only contain [a-zA-Z0-9_]", edge.End2Role()),
		}
	}

	if _, ok := edge.Attr(data.EdgeEnd2Cascading).(bool); !ok {
		return &util.GraphError{Type: util.ErrInvalidData, Detail: "Edge is missing a cascading value for end2"}
	}

	return nil
}

/*
writeNodeCount writes a new node count for a specific kind to the datastore.
*/
func (gm *Manager) writeNodeCount(kind string, count uint64, flush bool) error {
	numstr := make([]byte, 8)

	binary.LittleEndian.PutUint64(numstr, count)
	gm.gs.MainDB()[MainDBNodeCount+kind] = string(numstr)

	if flush {
		return gm.gs.FlushMain()
	}

	return nil
}

/*
writeEdgeCount writes a new edge count for a specific kind to the datastore.
*/
func (gm *Manager) writeEdgeCount(kind string, count uint64, flush bool) error {
	numstr := make([]byte, 8)

	binary.LittleEndian.PutUint64(numstr, count)
	gm.gs.MainDB()[MainDBEdgeCount+kind] = string(numstr)

	if flush {
		return gm.gs.FlushMain()
	}

	return nil
}

/*
getNodeStorageHTree gets two HTree instances which can be used to store nodes.
This function ensures that depending entries in other datastructures do exist.
*/
func (gm *Manager) getNodeStorageHTree(part string, kind string,
	create bool) (*hash.HTree, *hash.HTree, error) {

	gm.storageMutex.Lock()
	defer gm.storageMutex.Unlock()

	// Check if the partition name is valid

	if err := gm.checkPartitionName(part); err != nil {
		return nil, nil, err
	}

	// Check if the node kind is valid

	if !stringutil.IsAlphaNumeric(kind) {
		return nil, nil, &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("Node kind %v is not alphanumeric - can only contain [a-zA-Z0-9_]", kind),
		}
	}

	// Make sure all required lookup maps are there

	if gm.getMainDBMap(MainDBNodeKinds) == nil {
		gm.storeMainDBMap(MainDBNodeKinds, make(map[string]string))
	}

	if gm.getMainDBMap(MainDBParts) == nil {
		gm.storeMainDBMap(MainDBParts, make(map[string]string))
	}

	if gm.getMainDBMap(MainDBNodeAttrs+kind) == nil {
		gm.storeMainDBMap(MainDBNodeAttrs+kind, make(map[string]string))
	}

	if gm.getMainDBMap(MainDBNodeEdges+kind) == nil {
		gm.storeMainDBMap(MainDBNodeEdges+kind, make(map[string]string))
	}

	if _, ok := gm.gs.MainDB()[MainDBNodeCount+kind]; !ok {
		gm.gs.MainDB()[MainDBNodeCount+kind] = string(make([]byte, 8, 8))
	}

	// Return the actual storage

	gs := gm.gs.StorageManager(part+kind+StorageSuffixNodes, create)
	if gs == nil {
		return nil, nil, nil
	}

	attrTree, err := gm.getHTree(gs, RootIDNodeHTree)
	if err != nil {
		return nil, nil, err
	}

	valTree, err := gm.getHTree(gs, RootIDNodeHTreeSecond)
	if err != nil {
		return nil, nil, err
	}

	return attrTree, valTree, nil
}

/*
getEdgeStorageHTree gets a HTree which can be used to store edges. This function ensures that depending
entries in other datastructures do exist.
*/
func (gm *Manager) getEdgeStorageHTree(part string, kind string, create bool) (*hash.HTree, error) {

	gm.storageMutex.Lock()
	defer gm.storageMutex.Unlock()

	// Check if the partition name is valid

	if err := gm.checkPartitionName(part); err != nil {
		return nil, err
	}

	// Check if the edge kind is valid

	if !stringutil.IsAlphaNumeric(kind) {
		return nil, &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("Edge kind %v is not alphanumeric - can only contain [a-zA-Z0-9_]", kind),
		}
	}

	// Make sure all required lookup maps are there

	if gm.getMainDBMap(MainDBEdgeKinds) == nil {
		gm.storeMainDBMap(MainDBEdgeKinds, make(map[string]string))
	}

	if gm.getMainDBMap(MainDBEdgeAttrs+kind) == nil {
		gm.storeMainDBMap(MainDBEdgeAttrs+kind, make(map[string]string))
	}

	if _, ok := gm.gs.MainDB()[MainDBEdgeCount+kind]; !ok {
		gm.gs.MainDB()[MainDBEdgeCount+kind] = string(make([]byte, 8, 8))
	}

	// Return the actual storage

	gs := gm.gs.StorageManager(part+kind+StorageSuffixEdges, create)
	if gs == nil {
		return nil, nil
	}

	return gm.getHTree(gs, RootIDNodeHTree)
}

/*
getNodeIndexHTree gets a HTree which can be used to index nodes.
*/
func (gm *Manager) getNodeIndexHTree(part string, kind string, create bool) (*hash.HTree, error) {
	return gm.getIndexHTree(part, kind, create, "Node", StorageSuffixNodesIndex)
}

/*
getEdgeIndexHTree gets a HTree which can be used to index edges.
*/
func (gm *Manager) getEdgeIndexHTree(part string, kind string, create bool) (*hash.HTree, error) {
	return gm.getIndexHTree(part, kind, create, "Edge", StorageSuffixEdgesIndex)
}

/*
getIndexHTree gets a HTree which can be used to index items.
*/
func (gm *Manager) getIndexHTree(part string, kind string, create bool, name string, suffix string) (*hash.HTree, error) {

	gm.storageMutex.Lock()
	defer gm.storageMutex.Unlock()

	// Check if the partition name is valid

	if err := gm.checkPartitionName(part); err != nil {
		return nil, err
	}

	// Check if the kind is valid

	if !stringutil.IsAlphaNumeric(kind) {
		return nil, &util.GraphError{
			Type:   util.ErrInvalidData,
			Detail: fmt.Sprintf("%v kind %v is not alphanumeric - can only contain [a-zA-Z0-9_]", name, kind),
		}
	}

	gs := gm.gs.StorageManager(part+kind+suffix, create)
	if gs == nil {
		return nil, nil
	}

	return gm.getHTree(gs, RootIDNodeHTree)
}

/*
flushNodeStorage flushes a node storage.
*/
func (gm *Manager) flushNodeStorage(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixNodes, false); sm != nil {
		if err := sm.Flush(); err != nil {
			return &util.GraphError{Type: util.ErrFlushing, Detail: err.Error()}
		}
	}
	return nil
}

/*
flushNodeIndex flushes a node index.
*/
func (gm *Manager) flushNodeIndex(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixNodesIndex, false); sm != nil {
		if err := sm.Flush(); err != nil {
			return &util.GraphError{Type: util.ErrFlushing, Detail: err.Error()}
		}
	}
	return nil
}

/*
flushEdgeStorage flushes an edge storage.
*/
func (gm *Manager) flushEdgeStorage(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixEdges, false); sm != nil {
		if err := sm.Flush(); err != nil {
			return &util.GraphError{Type: util.ErrFlushing, Detail: err.Error()}
		}
	}
	return nil
}

/*
flushEdgeIndex flushes an edge index.
*/
func (gm *Manager) flushEdgeIndex(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixEdgesIndex, false); sm != nil {
		if err := sm.Flush(); err != nil {
			return &util.GraphError{Type: util.ErrFlushing, Detail: err.Error()}
		}
	}
	return nil
}

/*
rollbackNodeStorage rollbacks a node storage.
*/
func (gm *Manager) rollbackNodeStorage(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixNodes, false); sm != nil {
		if err := sm.Rollback(); err != nil {
			return &util.GraphError{Type: util.ErrRollback, Detail: err.Error()}
		}
	}
	return nil
}

/*
rollbackNodeIndex rollbacks a node index.
*/
func (gm *Manager) rollbackNodeIndex(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixNodesIndex, false); sm != nil {
		if err := sm.Rollback(); err != nil {
			return &util.GraphError{Type: util.ErrRollback, Detail: err.Error()}
		}
	}
	return nil
}

/*
rollbackEdgeStorage rollbacks an edge storage.
*/
func (gm *Manager) rollbackEdgeStorage(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixEdges, false); sm != nil {
		if err := sm.Rollback(); err != nil {
			return &util.GraphError{Type: util.ErrRollback, Detail: err.Error()}
		}
	}
	return nil
}

/*
rollbackEdgeIndex rollbacks an edge index.
*/
func (gm *Manager) rollbackEdgeIndex(part string, kind string) error {
	if sm := gm.gs.StorageManager(part+kind+StorageSuffixEdgesIndex, false); sm != nil {
		if err := sm.Rollback(); err != nil {
			return &util.GraphError{Type: util.ErrRollback, Detail: err.Error()}
		}
	}
	return nil
}

/*
getHTree creates or loads a HTree from a given StorageManager. HTrees are not cached
since the creation shouldn't have too much overhead.
*/
func (gm *Manager) getHTree(sm storage.Manager, slot int) (*hash.HTree, error) {
	var htree *hash.HTree
	var err error

	loc := sm.Root(slot)

	if loc == 0 {

		// Create a new HTree and store its location

		htree, err = hash.NewHTree(sm)

		if err != nil {
			err = &util.GraphError{Type: util.ErrAccessComponent, Detail: err.Error()}
		} else {
			sm.SetRoot(slot, htree.Location())
		}

	} else {

		// Load existing HTree

		htree, err = hash.LoadHTree(sm, loc)
		if err != nil {
			err = &util.GraphError{Type: util.ErrAccessComponent, Detail: err.Error()}
		}
	}

	return htree, err
}

/*
getMainDBMap gets a map from the main database.
*/
func (gm *Manager) getMainDBMap(key string) map[string]string {

	// First try to cache

	mapval, ok := gm.mapCache[key]
	if ok {
		return mapval
	}

	// Lookup map and decode it

	val, ok := gm.gs.MainDB()[key]
	if ok {
		mapval = stringToMap(val)
		gm.mapCache[key] = mapval
	}

	return mapval
}

/*
storeMainDBMap stores a map in the main database. The map is stored as a gob byte slice.
Once it has been decoded it is cached for read operations.
*/
func (gm *Manager) storeMainDBMap(key string, mapval map[string]string) {
	gm.mapCache[key] = mapval
	gm.gs.MainDB()[key] = mapToString(mapval)
}

// Static helper functions
// =======================

/*
IsFullSpec is a function to determine if a given spec is a fully specified spec
(i.e. all spec components are specified)
*/
func IsFullSpec(spec string) bool {
	sspec := strings.Split(spec, ":")

	if len(sspec) != 4 || sspec[0] == "" || sspec[1] == "" || sspec[2] == "" || sspec[3] == "" {
		return false
	}

	return true
}

/*
mapToString turns a map of strings into a single string.
*/
func mapToString(stringmap map[string]string) string {
	bb := &bytes.Buffer{}

	gob.NewEncoder(bb).Encode(stringmap)

	return string(bb.Bytes())
}

/*
stringToMap turns a string into a map of strings.
*/
func stringToMap(mapString string) map[string]string {
	var stringmap map[string]string

	if err := gob.NewDecoder(bytes.NewBufferString(mapString)).Decode(&stringmap); err != nil {
		panic(fmt.Sprint("Cannot decode:", mapString, err))
	}

	return stringmap
}
