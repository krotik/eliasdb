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
	"fmt"
	"strings"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/util"
)

/*
Trans data structure
*/
type Trans struct {
	gm       *Manager // Graph manager which created this transaction
	subtrans bool     // Flag if the transaction is a subtransaction

	storeNodes  map[string]data.Node // Nodes which should be stored
	removeNodes map[string]data.Node // Nodes which should be removed
	storeEdges  map[string]data.Edge // Edges which should be stored
	removeEdges map[string]data.Edge // Edges which should be removed
}

/*
NewGraphTrans creates a new graph transaction.
*/
func NewGraphTrans(gm *Manager) *Trans {
	return &Trans{gm, false, make(map[string]data.Node), make(map[string]data.Node),
		make(map[string]data.Edge), make(map[string]data.Edge)}
}

/*
IsEmpty returns if this transaction is empty.
*/
func (gt *Trans) IsEmpty() bool {
	return len(gt.storeNodes) == 0 && len(gt.removeNodes) == 0 &&
		len(gt.storeEdges) == 0 && len(gt.removeEdges) == 0
}

/*
Commit writes the transaction to the graph database. An automatic rollback is done if
any non-fatal error occurs. Failed transactions cannot be committed again.
Serious write errors which may corrupt the database will cause a panic.
*/
func (gt *Trans) Commit() error {

	// Take writer lock if we are not in a subtransaction

	if !gt.subtrans {
		gt.gm.mutex.Lock()
		defer gt.gm.mutex.Unlock()
	}

	// Return if there is nothing to do

	if gt.IsEmpty() {
		return nil
	}

	doRollback := func(nodePartsAndKinds map[string]string,
		edgePartsAndKinds map[string]string) {

		// Rollback main database

		gt.gm.gs.RollbackMain()

		// Rollback node storages

		for kkey := range nodePartsAndKinds {
			partAndKind := strings.Split(kkey, "#")

			gt.gm.rollbackNodeIndex(partAndKind[0], partAndKind[1])
			gt.gm.rollbackNodeStorage(partAndKind[0], partAndKind[1])
		}

		gt.storeNodes = make(map[string]data.Node)
		gt.removeNodes = make(map[string]data.Node)

		// Rollback edge storages

		if edgePartsAndKinds != nil {
			for kkey := range edgePartsAndKinds {
				partAndKind := strings.Split(kkey, "#")

				gt.gm.rollbackEdgeIndex(partAndKind[0], partAndKind[1])
				gt.gm.rollbackEdgeStorage(partAndKind[0], partAndKind[1])
			}
		}

		gt.storeEdges = make(map[string]data.Edge)
		gt.removeEdges = make(map[string]data.Edge)
	}

	// Write nodes and edges until everything has been written

	nodePartsAndKinds := make(map[string]string)
	edgePartsAndKinds := make(map[string]string)

	for !gt.IsEmpty() {

		// Write the nodes first

		if err := gt.commitNodes(nodePartsAndKinds, edgePartsAndKinds); err != nil {
			doRollback(nodePartsAndKinds, nil)
			return err
		}

		// After the nodes write the edges

		if err := gt.commitEdges(nodePartsAndKinds, edgePartsAndKinds); err != nil {
			doRollback(nodePartsAndKinds, edgePartsAndKinds)
			return err
		}
	}

	// Flush changes - panic instead of error reporting since the database
	// may be inconsistent

	panicIfError := func(err error) {
		if err != nil {
			panic("Fatal GraphError:" + err.Error())
		}
	}

	panicIfError(gt.gm.gs.FlushMain())

	for kkey := range nodePartsAndKinds {

		partAndKind := strings.Split(kkey, "#")

		panicIfError(gt.gm.flushNodeIndex(partAndKind[0], partAndKind[1]))
		panicIfError(gt.gm.flushNodeStorage(partAndKind[0], partAndKind[1]))
	}

	for kkey := range edgePartsAndKinds {

		partAndKind := strings.Split(kkey, "#")

		panicIfError(gt.gm.flushEdgeIndex(partAndKind[0], partAndKind[1]))
		panicIfError(gt.gm.flushEdgeStorage(partAndKind[0], partAndKind[1]))
	}

	return nil
}

/*
commitNodes tries to commit all transaction nodes.
*/
func (gt *Trans) commitNodes(nodePartsAndKinds map[string]string, edgePartsAndKinds map[string]string) error {

	// First insert nodes

	for tkey, node := range gt.storeNodes {

		// Get partition and kind

		partAndKind := strings.Split(tkey, "#")
		nodePartsAndKinds[partAndKind[0]+"#"+partAndKind[1]] = ""

		part := partAndKind[0]

		// Get the HTrees which stores the node index and node

		iht, err := gt.gm.getNodeIndexHTree(part, node.Kind(), true)
		if err != nil {
			return err
		}

		attht, valht, err := gt.gm.getNodeStorageHTree(part, node.Kind(), true)
		if err != nil || attht == nil || valht == nil {
			return err
		}

		// Write the node to the datastore

		oldnode, err := gt.gm.writeNode(node, false, attht, valht, nodeAttributeFilter)

		if err != nil {
			return err
		}

		// Increase node count if the node was inserted and write the changes
		// to the index.

		if oldnode == nil {
			currentCount := gt.gm.NodeCount(node.Kind())
			gt.gm.writeNodeCount(node.Kind(), currentCount+1, false)

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

		// Execute rules

		var event int
		if oldnode == nil {
			event = EventNodeCreated
		} else {
			event = EventNodeUpdated
		}

		if err := gt.gm.gr.graphEvent(gt, event, part, node, oldnode); err != nil {
			return err
		}

		delete(gt.storeNodes, tkey)
	}

	// Then remove nodes

	for tkey, node := range gt.removeNodes {

		// Get partition and kind

		partAndKind := strings.Split(tkey, "#")
		nodePartsAndKinds[partAndKind[0]+"#"+partAndKind[1]] = ""

		part := partAndKind[0]

		// Get the HTree which stores the node index and node kind

		iht, err := gt.gm.getNodeIndexHTree(part, node.Kind(), false)
		if err != nil {
			return err
		}

		attTree, valTree, err := gt.gm.getNodeStorageHTree(part, node.Kind(), false)
		if err != nil || attTree == nil || valTree == nil {
			return err
		}

		// Delete the node from the datastore

		oldnode, err := gt.gm.deleteNode(node.Key(), node.Kind(), attTree, valTree)
		if err != nil {
			return err
		}

		// Update the index

		if oldnode != nil {

			if iht != nil {
				err := util.NewIndexManager(iht).Deindex(node.Key(), oldnode.IndexMap())

				if err != nil {
					return err
				}
			}

			// Decrease the node count

			currentCount := gt.gm.NodeCount(node.Kind())
			gt.gm.writeNodeCount(node.Kind(), currentCount-1, false)

			// Execute rules

			if err := gt.gm.gr.graphEvent(gt, EventNodeDeleted, part, oldnode); err != nil {
				return err
			}
		}

		delete(gt.removeNodes, tkey)
	}

	return nil
}

/*
commitEdges tries to commit all transaction edges.
*/
func (gt *Trans) commitEdges(nodePartsAndKinds map[string]string, edgePartsAndKinds map[string]string) error {

	// First insert edges

	for tkey, edge := range gt.storeEdges {

		// Get partition and kind

		partAndKind := strings.Split(tkey, "#")
		edgePartsAndKinds[partAndKind[0]+"#"+partAndKind[1]] = ""

		nodePartsAndKinds[partAndKind[0]+"#"+edge.End1Kind()] = ""
		nodePartsAndKinds[partAndKind[0]+"#"+edge.End2Kind()] = ""

		part := partAndKind[0]

		// Get the HTrees which stores the edges and the edge index

		iht, err := gt.gm.getEdgeIndexHTree(part, edge.Kind(), true)
		if err != nil {
			return err
		}

		edgeht, err := gt.gm.getEdgeStorageHTree(part, edge.Kind(), true)
		if err != nil {
			return err
		}

		// Get the HTrees which stores the edge endpoints and make sure the endpoints
		// do exist

		end1nodeht, end1ht, err := gt.gm.getNodeStorageHTree(part, edge.End1Kind(), false)

		if err != nil {
			return err
		} else if end1ht == nil {
			return &util.GraphError{
				Type:   util.ErrInvalidData,
				Detail: fmt.Sprintf("Can't store edge to non-existend node kind: %v", edge.End1Kind()),
			}
		} else if end1, err := end1nodeht.Get([]byte(PrefixNSAttrs + edge.End1Key())); err != nil || end1 == nil {
			return &util.GraphError{
				Type:   util.ErrInvalidData,
				Detail: fmt.Sprintf("Can't find edge endpoint: %s (%s)", edge.End1Key(), edge.End1Kind()),
			}
		}

		end2nodeht, end2ht, err := gt.gm.getNodeStorageHTree(part, edge.End2Kind(), false)

		if err != nil {
			return err
		} else if end2ht == nil {
			return &util.GraphError{
				Type:   util.ErrInvalidData,
				Detail: "Can't store edge to non-existend node kind: " + edge.End2Kind()}
		} else if end2, err := end2nodeht.Get([]byte(PrefixNSAttrs + edge.End2Key())); err != nil || end2 == nil {
			return &util.GraphError{
				Type:   util.ErrInvalidData,
				Detail: fmt.Sprintf("Can't find edge endpoint: %s (%s)", edge.End2Key(), edge.End2Kind()),
			}
		}

		// Write edge to the datastore

		oldedge, err := gt.gm.writeEdge(edge, edgeht, end1ht, end2ht)
		if err != nil {
			return err
		}

		// Increase edge count if the edge was inserted and write the changes
		// to the index.

		if oldedge == nil {

			// Increase edge count

			currentCount := gt.gm.EdgeCount(edge.Kind())
			gt.gm.writeEdgeCount(edge.Kind(), currentCount+1, false)

			// Write edge data to the index

			if iht != nil {

				if err := util.NewIndexManager(iht).Index(edge.Key(), edge.IndexMap()); err != nil {

					// The edge was written at this point and the model is
					// consistent only the index is missing entries

					return err
				}
			}

		} else if iht != nil {

			err := util.NewIndexManager(iht).Reindex(edge.Key(), edge.IndexMap(),
				oldedge.IndexMap())

			if err != nil {

				// The edge was written at this point and the model is
				// consistent only the index is missing entries

				return err
			}
		}

		// Execute rules

		var event int
		if oldedge == nil {
			event = EventEdgeCreated
		} else {
			event = EventEdgeUpdated
		}

		if err := gt.gm.gr.graphEvent(gt, event, part, edge, oldedge); err != nil {
			return err
		}

		delete(gt.storeEdges, tkey)
	}

	// Then remove edges

	for tkey, edge := range gt.removeEdges {

		// Get partition and kind

		partAndKind := strings.Split(tkey, "#")
		edgePartsAndKinds[partAndKind[0]+"#"+partAndKind[1]] = ""

		nodePartsAndKinds[partAndKind[0]+"#"+edge.End1Kind()] = ""
		nodePartsAndKinds[partAndKind[0]+"#"+edge.End2Kind()] = ""

		part := partAndKind[0]

		// Get the HTrees which stores the edges and the edge index

		iht, err := gt.gm.getEdgeIndexHTree(part, edge.Kind(), true)
		if err != nil {
			return err
		}

		edgeht, err := gt.gm.getEdgeStorageHTree(part, edge.Kind(), true)
		if err != nil {
			return err
		}

		// Delete the node from the datastore

		node, err := gt.gm.deleteNode(edge.Key(), edge.Kind(), edgeht, edgeht)
		oldedge := data.NewGraphEdgeFromNode(node)
		if err != nil {
			return err
		}

		if node != nil {

			// Get the HTrees which stores the edge endpoints

			_, end1ht, err := gt.gm.getNodeStorageHTree(part, oldedge.End1Kind(), false)
			if err != nil {
				return err
			}

			_, end2ht, err := gt.gm.getNodeStorageHTree(part, oldedge.End2Kind(), false)
			if err != nil {
				return err
			}

			// Delete edge info from node storage

			if err := gt.gm.deleteEdge(oldedge, end1ht, end2ht); err != nil {
				return err
			}

			if iht != nil {

				err := util.NewIndexManager(iht).Deindex(edge.Key(), oldedge.IndexMap())
				if err != nil {
					return err
				}
			}

			// Decrease edge count

			currentCount := gt.gm.EdgeCount(oldedge.Kind())
			gt.gm.writeEdgeCount(oldedge.Kind(), currentCount-1, false)

			// Execute rules

			if err := gt.gm.gr.graphEvent(gt, EventEdgeDeleted, part, oldedge); err != nil {
				return err
			}
		}

		delete(gt.removeEdges, tkey)
	}

	return nil
}

/*
StoreNode stores a single node in a partition of the graph. This function will
overwrites any existing node.
*/
func (gt *Trans) StoreNode(part string, node data.Node) error {
	if err := gt.gm.checkPartitionName(part); err != nil {
		return err
	} else if err := gt.gm.checkNode(node); err != nil {
		return err
	}

	key := gt.createKey(part, node.Key(), node.Kind())

	if _, ok := gt.removeNodes[key]; ok {
		delete(gt.removeNodes, key)
	}

	gt.storeNodes[key] = node

	return nil
}

/*
UpdateNode updates a single node in a partition of the graph. This function will
only update the given values of the node.
*/
func (gt *Trans) UpdateNode(part string, node data.Node) error {
	if err := gt.gm.checkPartitionName(part); err != nil {
		return err
	} else if err := gt.gm.checkNode(node); err != nil {
		return err
	}

	key := gt.createKey(part, node.Key(), node.Kind())

	if _, ok := gt.removeNodes[key]; ok {
		delete(gt.removeNodes, key)
	} else if storeNode, ok := gt.storeNodes[key]; ok {
		node = data.NodeMerge(storeNode, node)
	} else {

		// Check the actual database if the node exists

		storeNode, err := gt.gm.FetchNode(part, node.Key(), node.Kind())
		if err != nil {
			return err
		} else if storeNode != nil {
			node = data.NodeMerge(storeNode, node)
		}
	}

	gt.storeNodes[key] = node

	return nil
}

/*
RemoveNode removes a single node from a partition of the graph.
*/
func (gt *Trans) RemoveNode(part string, nkey string, nkind string) error {
	if err := gt.gm.checkPartitionName(part); err != nil {
		return err
	}

	key := gt.createKey(part, nkey, nkind)

	if _, ok := gt.storeNodes[key]; ok {
		delete(gt.storeNodes, key)
	}

	node := data.NewGraphNode()
	node.SetAttr(data.NodeKey, nkey)
	node.SetAttr(data.NodeKind, nkind)

	gt.removeNodes[key] = node

	return nil
}

/*
StoreEdge stores a single edge in a partition of the graph. This function will
overwrites any existing edge.
*/
func (gt *Trans) StoreEdge(part string, edge data.Edge) error {
	if err := gt.gm.checkPartitionName(part); err != nil {
		return err
	} else if err := gt.gm.checkEdge(edge); err != nil {
		return err
	}

	key := gt.createKey(part, edge.Key(), edge.Kind())

	if _, ok := gt.removeEdges[key]; ok {
		delete(gt.removeEdges, key)
	}

	gt.storeEdges[key] = edge

	return nil
}

/*
RemoveEdge removes a single edge from a partition of the graph.
*/
func (gt *Trans) RemoveEdge(part string, ekey string, ekind string) error {
	if err := gt.gm.checkPartitionName(part); err != nil {
		return err
	}

	key := gt.createKey(part, ekey, ekind)

	if _, ok := gt.storeEdges[key]; ok {
		delete(gt.storeEdges, key)
	}

	edge := data.NewGraphEdge()
	edge.SetAttr(data.NodeKey, ekey)
	edge.SetAttr(data.NodeKind, ekind)

	gt.removeEdges[key] = edge

	return nil
}

/*
Create a key for the transaction storage.
*/
func (gt *Trans) createKey(part string, key string, kind string) string {
	return part + "#" + kind + "#" + key
}
