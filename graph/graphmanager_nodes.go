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
	"encoding/binary"
	"encoding/gob"

	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/util"
	"devt.de/krotik/eliasdb/hash"
)

func init() {

	// It is possible to store nested structures on nodes

	gob.Register(make(map[string]interface{}))
}

/*
NodeCount returns the node count for a given node kind.
*/
func (gm *Manager) NodeCount(kind string) uint64 {

	if val, ok := gm.gs.MainDB()[MainDBNodeCount+kind]; ok {
		return binary.LittleEndian.Uint64([]byte(val))
	}

	return 0
}

/*
NodeKeyIterator iterates node keys of a certain kind.
*/
func (gm *Manager) NodeKeyIterator(part string, kind string) (*NodeKeyIterator, error) {
	// Get the HTrees which stores the node

	tree, _, err := gm.getNodeStorageHTree(part, kind, false)
	if err != nil || tree == nil {
		return nil, err
	}

	it := hash.NewHTreeIterator(tree)
	if it.LastError != nil {
		return nil, &util.GraphError{
			Type:   util.ErrReading,
			Detail: it.LastError.Error(),
		}
	}

	return &NodeKeyIterator{gm, it, nil}, nil
}

/*
FetchNode fetches a single node from a partition of the graph.
*/
func (gm *Manager) FetchNode(part string, key string, kind string) (data.Node, error) {
	return gm.FetchNodePart(part, key, kind, nil)
}

/*
FetchNodePart fetches part of a single node from a partition of the graph.
*/
func (gm *Manager) FetchNodePart(part string, key string, kind string,
	attrs []string) (data.Node, error) {

	// Get the HTrees which stores the node

	attht, valht, err := gm.getNodeStorageHTree(part, kind, false)
	if err != nil || attht == nil || valht == nil {
		return nil, err
	}

	// Take reader lock

	gm.mutex.RLock()
	defer gm.mutex.RUnlock()

	// Read the node from the datastore

	return gm.readNode(key, kind, attrs, attht, valht)
}

/*
readNode reads a given node from the datastore.
*/
func (gm *Manager) readNode(key string, kind string, attrs []string,
	attrTree *hash.HTree, valTree *hash.HTree) (data.Node, error) {

	keyAttrs := PrefixNSAttrs + key
	keyAttrPrefix := PrefixNSAttr + key

	// Check if the node exists

	attrList, err := attrTree.Get([]byte(keyAttrs))
	if err != nil {
		return nil, &util.GraphError{Type: util.ErrReading, Detail: err.Error()}
	} else if attrList == nil {
		return nil, nil
	}

	var node data.Node

	tryPopulateNode := func(encattr string, attr string) error {

		// Try to lookup the attribute

		val, err := valTree.Get([]byte(keyAttrPrefix + encattr))
		if err != nil {
			return &util.GraphError{Type: util.ErrReading, Detail: err.Error()}
		}

		if val != nil {
			if node == nil {
				node = data.NewGraphNode()
			}
			node.SetAttr(attr, val)
		}

		return nil
	}

	if len(attrs) == 0 {

		// Allways create a node if we fetch all attributes

		node = data.NewGraphNode()

		// Lookup all attributes

		for _, encattr := range attrList.([]string) {
			attr := gm.nm.Decode32(encattr)
			if err := tryPopulateNode(encattr, attr); err != nil {
				return nil, err
			}
		}

	} else {

		// Lookup the given attributes - it is assumed that most of the time the
		// queried attributes do exist

		for _, attr := range attrs {

			if (attr == data.NodeKey || attr == data.NodeKind) && node == nil {

				// Create node - we might only query for node key or node kind

				node = data.NewGraphNode()
				continue
			}

			// Only try to populate the attribute if it can be decoded

			if encattr := gm.nm.Encode32(attr, false); encattr != "" {
				if err := tryPopulateNode(encattr, attr); err != nil {
					return nil, err
				}
			}
		}
	}

	// Set key and kind attributes

	if node != nil {
		node.SetAttr(data.NodeKey, key)
		node.SetAttr(data.NodeKind, kind)
	}

	return node, nil
}

/*
StoreNode stores a single node in a partition of the graph. This function will
overwrites any existing node.
*/
func (gm *Manager) StoreNode(part string, node data.Node) error {
	trans := newInternalGraphTrans(gm)
	trans.subtrans = true

	err := gm.gr.graphEvent(trans, EventNodeStore, part, node)

	if err != nil {
		if err == ErrEventHandled {
			err = nil
		}
		return err
	}

	if err = trans.Commit(); err == nil {
		err = gm.storeOrUpdateNode(part, node, false)
	}

	return err
}

/*
UpdateNode updates a single node in a partition of the graph. This function will
only update the given values of the node.
*/
func (gm *Manager) UpdateNode(part string, node data.Node) error {
	trans := newInternalGraphTrans(gm)
	trans.subtrans = true

	err := gm.gr.graphEvent(trans, EventNodeUpdate, part, node)

	if err != nil {
		if err == ErrEventHandled {
			err = nil
		}
		return err
	}

	if err = trans.Commit(); err == nil {
		err = gm.storeOrUpdateNode(part, node, true)
	}

	return err
}

/*
storeOrUpdateNode stores or updates a single node in a partition of the graph.
*/
func (gm *Manager) storeOrUpdateNode(part string, node data.Node, onlyUpdate bool) error {

	// Check if the node can be stored

	if err := gm.checkNode(node); err != nil {
		return err
	}

	// Get the HTrees which stores the node index and node

	iht, err := gm.getNodeIndexHTree(part, node.Kind(), true)
	if err != nil {
		return err
	}

	attht, valht, err := gm.getNodeStorageHTree(part, node.Kind(), true)
	if err != nil || attht == nil || valht == nil {
		return err
	}

	// Take writer lock

	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	// Write the node to the datastore

	oldnode, err := gm.writeNode(node, onlyUpdate, attht, valht, nodeAttributeFilter)
	if err != nil {
		return err
	}

	// Increase node count if the node was inserted and write the changes
	// to the index.

	if oldnode == nil {
		currentCount := gm.NodeCount(node.Kind())
		if err := gm.writeNodeCount(node.Kind(), currentCount+1, true); err != nil {
			return err
		}

		if iht != nil {
			err := util.NewIndexManager(iht).Index(node.Key(), node.IndexMap())
			if err != nil {

				// The node was written at this point and the model is
				// consistent only the index is missing entries

				return err
			}
		}

	} else if iht != nil {

		err := util.NewIndexManager(iht).Reindex(node.Key(), node.IndexMap(),
			oldnode.IndexMap())

		if err != nil {

			// The node was written at this point and the model is
			// consistent only the index is missing entries

			return err
		}
	}

	defer func() {

		// Flush changes

		gm.gs.FlushMain()

		gm.flushNodeIndex(part, node.Kind())

		gm.flushNodeStorage(part, node.Kind())

	}()

	// Execute rules

	trans := newInternalGraphTrans(gm)
	trans.subtrans = true

	var event int
	if oldnode == nil {
		event = EventNodeCreated
	} else {
		event = EventNodeUpdated
	}

	if err := gm.gr.graphEvent(trans, event, part, node, oldnode); err != nil && err != ErrEventHandled {
		return err
	} else if err := trans.Commit(); err != nil {
		return err
	}

	return nil
}

/*
writeNode writes a given node in full or part to the datastore. It is assumed
that the caller holds the writer lock before calling the functions and that,
after the function returns, the changes are flushed to the storage. Returns
the old node if an update occurred. An attribute filter can be speified to skip
specific attributes.
*/
func (gm *Manager) writeNode(node data.Node, onlyUpdate bool, attrTree *hash.HTree,
	valTree *hash.HTree, attFilter func(attr string) bool) (data.Node, error) {

	keyAttrs := PrefixNSAttrs + node.Key()
	keyAttrPrefix := PrefixNSAttr + node.Key()

	var oldnode data.Node
	var attrListOld interface{}
	var err error

	// Store the node attributes

	attrList := make([]string, 0, len(node.IndexMap()))
	attrMap := make(map[string]string)

	for attr, val := range node.Data() {

		// Ignore filtered attributes

		if attFilter(attr) {
			continue
		}

		encattr := gm.nm.Encode32(attr, true)

		// Build up a lookup map to identify which attribute exist

		attrMap[encattr] = ""

		// Build up new attributes list

		attrList = append(attrList, encattr)

		// Store the value in the datastore

		oldval, err := valTree.Put([]byte(keyAttrPrefix+encattr), val)
		if err != nil {
			return nil, &util.GraphError{Type: util.ErrWriting, Detail: err.Error()}
		}

		// Build up old node

		if oldval != nil {
			if oldnode == nil {
				oldnode = data.NewGraphNode()
			}
			oldnode.SetAttr(attr, oldval)
		}
	}

	if onlyUpdate {

		// If we do only an update write the attribute list only if we added
		// new attributes

		attrListOld, err = attrTree.Get([]byte(keyAttrs))
		if err != nil {
			return nil, &util.GraphError{Type: util.ErrReading, Detail: err.Error()}
		}

		if attrListOld != nil {

			// Fill up the attrMap with the existing attributes

			for _, encattr := range attrListOld.([]string) {
				attrMap[encattr] = ""
			}

			// Now check if we need to write the attribute list

			if len(attrListOld.([]string)) != len(attrMap) {

				// Store the new node attributes

				attrList = make([]string, 0, len(attrMap))
				for encattr := range attrMap {
					attrList = append(attrList, encattr)
				}

				attrListOld, err = attrTree.Put([]byte(keyAttrs), attrList)
			}

		} else {

			// We are actually doing an insert - just write the attribute list

			_, err = attrTree.Put([]byte(keyAttrs), attrList)
		}

	} else {

		// Store the new node attributes

		attrListOld, err = attrTree.Put([]byte(keyAttrs), attrList)
	}

	if err != nil {

		// Do not try cleanup in case we updated a node - we would do more
		// harm than good.

		return nil, &util.GraphError{Type: util.ErrWriting, Detail: err.Error()}
	}

	// Remove deleted keys

	if attrListOld != nil {

		// Create old node if non of the new attributes were on the old node

		if oldnode == nil {
			oldnode = data.NewGraphNode()
		}

		oldnode.SetAttr(data.NodeKey, node.Key())
		oldnode.SetAttr(data.NodeKind, node.Kind())

		for _, encattrold := range attrListOld.([]string) {

			if _, ok := attrMap[encattrold]; !ok {

				oldval, err := valTree.Remove([]byte(keyAttrPrefix + encattrold))
				if err != nil {
					return nil, &util.GraphError{Type: util.ErrWriting, Detail: err.Error()}
				}

				oldnode.SetAttr(gm.nm.Decode32(encattrold), oldval)
			}
		}

		return oldnode, nil
	}

	return nil, nil
}

/*
RemoveNode removes a single node from a partition of the graph.
*/
func (gm *Manager) RemoveNode(part string, key string, kind string) (data.Node, error) {
	var err error

	trans := newInternalGraphTrans(gm)
	trans.subtrans = true

	if err = gm.gr.graphEvent(trans, EventNodeDelete, part, key, kind); err != nil {
		if err == ErrEventHandled {
			err = nil
		}
		return nil, err
	}

	err = trans.Commit()

	if err == nil {

		// Get the HTree which stores the node index and node kind

		iht, err := gm.getNodeIndexHTree(part, kind, false)
		if err != nil {
			return nil, err
		}

		attTree, valTree, err := gm.getNodeStorageHTree(part, kind, false)
		if err != nil || attTree == nil || valTree == nil {
			return nil, err
		}

		// Take writer lock

		gm.mutex.Lock()
		defer gm.mutex.Unlock()

		// Delete the node from the datastore

		node, err := gm.deleteNode(key, kind, attTree, valTree)
		if err != nil {
			return node, err
		}

		// Update the index

		if node != nil {

			if iht != nil {
				err := util.NewIndexManager(iht).Deindex(key, node.IndexMap())
				if err != nil {
					return node, err
				}
			}

			// Decrease the node count

			currentCount := gm.NodeCount(kind)
			if err := gm.writeNodeCount(kind, currentCount-1, true); err != nil {
				return node, err
			}

			defer func() {

				// Flush changes

				gm.gs.FlushMain()

				gm.flushNodeIndex(part, kind)

				gm.flushNodeStorage(part, kind)
			}()

			// Execute rules

			trans := newInternalGraphTrans(gm)
			trans.subtrans = true

			if err := gm.gr.graphEvent(trans, EventNodeDeleted, part, node); err != nil && err != ErrEventHandled {
				return node, err
			} else if err := trans.Commit(); err != nil {
				return node, err
			}

			return node, nil
		}
	}

	return nil, err
}

/*
deleteNode deletes a given node from the datastore. It is assumed that the caller
holds the writer lock before calling the functions and that, after the function
returns, the changes are flushed to the storage. Returns the deleted node.
*/
func (gm *Manager) deleteNode(key string, kind string, attrTree *hash.HTree,
	valTree *hash.HTree) (data.Node, error) {

	keyAttrs := PrefixNSAttrs + key
	keyAttrPrefix := PrefixNSAttr + key

	// Remove the attribute list entry

	attrList, err := attrTree.Remove([]byte(keyAttrs))
	if err != nil {
		return nil, &util.GraphError{Type: util.ErrWriting, Detail: err.Error()}
	} else if attrList == nil {
		return nil, nil
	}

	// Create the node object which is returned

	node := data.NewGraphNode()

	node.SetAttr(data.NodeKey, key)
	node.SetAttr(data.NodeKind, kind)

	// Remove node attributes

	for _, encattr := range attrList.([]string) {
		attr := gm.nm.Decode32(encattr)

		// Try to remove the attribute

		val, err := valTree.Remove([]byte(keyAttrPrefix + encattr))
		if err != nil {
			return node, &util.GraphError{Type: util.ErrWriting, Detail: err.Error()}
		}

		node.SetAttr(attr, val)
	}

	return node, nil
}

/*
Default filter function to filter out system node attributes.
*/
func nodeAttributeFilter(attr string) bool {
	return attr == data.NodeKey || attr == data.NodeKind
}
