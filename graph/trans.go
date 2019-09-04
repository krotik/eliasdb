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
	"sync"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/util"
)

/*
Trans is a transaction object which should be used to group node and edge operations.
*/
type Trans interface {

	/*
	   ID returns a unique transaction ID.
	*/
	ID() string

	/*
	   String returns a string representation of this transatction.
	*/
	String() string

	/*
	   Counts returns the transaction size in terms of objects. Returned values
	   are nodes to store, edges to store, nodes to remove and edges to remove.
	*/
	Counts() (int, int, int, int)

	/*
	   IsEmpty returns if this transaction is empty.
	*/
	IsEmpty() bool

	/*
	   Commit writes the transaction to the graph database. An automatic rollback is done if
	   any non-fatal error occurs. Failed transactions cannot be committed again.
	   Serious write errors which may corrupt the database will cause a panic.
	*/
	Commit() error

	/*
	   StoreNode stores a single node in a partition of the graph. This function will
	   overwrites any existing node.
	*/
	StoreNode(part string, node data.Node) error

	/*
	   UpdateNode updates a single node in a partition of the graph. This function will
	   only update the given values of the node.
	*/
	UpdateNode(part string, node data.Node) error

	/*
	   RemoveNode removes a single node from a partition of the graph.
	*/
	RemoveNode(part string, nkey string, nkind string) error

	/*
	   StoreEdge stores a single edge in a partition of the graph. This function will
	   overwrites any existing edge.
	*/
	StoreEdge(part string, edge data.Edge) error

	/*
	   RemoveEdge removes a single edge from a partition of the graph.
	*/
	RemoveEdge(part string, ekey string, ekind string) error
}

/*
NewGraphTrans creates a new graph transaction. This object is not thread safe
and should only be used for non-concurrent use cases; use NewConcurrentGraphTrans
for concurrent use cases.
*/
func NewGraphTrans(gm *Manager) Trans {
	return newInternalGraphTrans(gm)
}

/*
NewConcurrentGraphTrans creates a new thread-safe graph transaction.
*/
func NewConcurrentGraphTrans(gm *Manager) Trans {
	return &concurrentTrans{NewGraphTrans(gm), &sync.RWMutex{}}
}

/*
NewRollingTrans wraps an existing transaction into a rolling transaction.
Rolling transactions can be used for VERY large datasets and will commit
themselves after n operations. Rolling transactions are always thread-safe.
*/
func NewRollingTrans(t Trans, n int, gm *Manager, newTrans func(*Manager) Trans) Trans {
	idCounterLock.Lock()
	defer idCounterLock.Unlock()

	idCounter++

	// Smallest commit threshold is 1

	if n < 1 {
		n = 1
	}

	return &rollingTrans{

		id: fmt.Sprint(idCounter),
		gm: gm,

		currentTrans: t,
		newTransFunc: newTrans,
		transErrors:  errorutil.NewCompositeError(),

		opThreshold:   n,
		opCount:       0,
		inFlightCount: 0,
		wg:            &sync.WaitGroup{},

		countNodeIns: 0,
		countNodeRem: 0,
		countEdgeIns: 0,
		countEdgeRem: 0,

		transLock: &sync.RWMutex{},
	}
}

/*
newInternalGraphTrans is used for internal transactions. The returned object
contains extra fields which are only for internal use.
*/
func newInternalGraphTrans(gm *Manager) *baseTrans {
	idCounterLock.Lock()
	defer idCounterLock.Unlock()

	idCounter++

	return &baseTrans{fmt.Sprint(idCounter), gm, false, make(map[string]data.Node), make(map[string]data.Node),
		make(map[string]data.Edge), make(map[string]data.Edge)}
}

/*
idCounter is a simple counter for ids
*/
var idCounter uint64
var idCounterLock = &sync.Mutex{}

/*
baseTrans is the main data structure for a graph transaction
*/
type baseTrans struct {
	id       string   // Unique transaction ID - not used by EliasDB
	gm       *Manager // Graph manager which created this transaction
	subtrans bool     // Flag if the transaction is a subtransaction

	storeNodes  map[string]data.Node // Nodes which should be stored
	removeNodes map[string]data.Node // Nodes which should be removed
	storeEdges  map[string]data.Edge // Edges which should be stored
	removeEdges map[string]data.Edge // Edges which should be removed
}

/*
ID returns a unique transaction ID.
*/
func (gt *baseTrans) ID() string {
	return gt.id
}

/*
IsEmpty returns if this transaction is empty.
*/
func (gt *baseTrans) IsEmpty() bool {
	sn, se, rn, re := gt.Counts()

	return sn == 0 && se == 0 && rn == 0 && re == 0
}

/*
Counts returns the transaction size in terms of objects. Returned values
are nodes to store, edges to store, nodes to remove and edges to remove.
*/
func (gt *baseTrans) Counts() (int, int, int, int) {
	return len(gt.storeNodes), len(gt.storeEdges), len(gt.removeNodes), len(gt.removeEdges)
}

/*
String returns a string representation of this transatction.
*/
func (gt *baseTrans) String() string {
	sn, se, rn, re := gt.Counts()

	return fmt.Sprintf("Transaction %v - Nodes: I:%v R:%v - Edges: I:%v R:%v",
		gt.id, sn, rn, se, re)
}

/*
Commit writes the transaction to the graph database. An automatic rollback is done if
any non-fatal error occurs. Failed transactions cannot be committed again.
Serious write errors which may corrupt the database will cause a panic.
*/
func (gt *baseTrans) Commit() error {

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
func (gt *baseTrans) commitNodes(nodePartsAndKinds map[string]string, edgePartsAndKinds map[string]string) error {

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
		if err != nil {
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
		if err != nil {
			return err
		}

		if attTree == nil || valTree == nil {

			// Kind does not exist - continue

			delete(gt.removeNodes, tkey)
			continue
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
func (gt *baseTrans) commitEdges(nodePartsAndKinds map[string]string, edgePartsAndKinds map[string]string) error {

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
				Detail: fmt.Sprintf("Can't store edge to non-existing node kind: %v", edge.End1Kind()),
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
				Detail: "Can't store edge to non-existing node kind: " + edge.End2Kind()}
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
func (gt *baseTrans) StoreNode(part string, node data.Node) error {
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
func (gt *baseTrans) UpdateNode(part string, node data.Node) error {
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
func (gt *baseTrans) RemoveNode(part string, nkey string, nkind string) error {
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
func (gt *baseTrans) StoreEdge(part string, edge data.Edge) error {
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
func (gt *baseTrans) RemoveEdge(part string, ekey string, ekind string) error {
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
func (gt *baseTrans) createKey(part string, key string, kind string) string {
	return part + "#" + kind + "#" + key
}

/*
concurrentTrans is a lock-wrapper around baseTrans which allows concurrent use.
*/
type concurrentTrans struct {
	Trans
	transLock *sync.RWMutex
}

/*
ID returns a unique transaction ID.
*/
func (gt *concurrentTrans) ID() string {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	return gt.Trans.ID()
}

/*
String returns a string representation of this transatction.
*/
func (gt *concurrentTrans) String() string {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	return gt.Trans.String()
}

/*
Counts returns the transaction size in terms of objects. Returned values
are nodes to store, edges to store, nodes to remove and edges to remove.
*/
func (gt *concurrentTrans) Counts() (int, int, int, int) {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	return gt.Trans.Counts()
}

/*
IsEmpty returns if this transaction is empty.
*/
func (gt *concurrentTrans) IsEmpty() bool {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	return gt.Trans.IsEmpty()
}

/*
Commit writes the transaction to the graph database. An automatic rollback is done if
any non-fatal error occurs. Failed transactions cannot be committed again.
Serious write errors which may corrupt the database will cause a panic.
*/
func (gt *concurrentTrans) Commit() error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	return gt.Trans.Commit()
}

/*
StoreNode stores a single node in a partition of the graph. This function will
overwrites any existing node.
*/
func (gt *concurrentTrans) StoreNode(part string, node data.Node) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	return gt.Trans.StoreNode(part, node)
}

/*
UpdateNode updates a single node in a partition of the graph. This function will
only update the given values of the node.
*/
func (gt *concurrentTrans) UpdateNode(part string, node data.Node) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	return gt.Trans.UpdateNode(part, node)
}

/*
RemoveNode removes a single node from a partition of the graph.
*/
func (gt *concurrentTrans) RemoveNode(part string, nkey string, nkind string) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	return gt.Trans.RemoveNode(part, nkey, nkind)
}

/*
StoreEdge stores a single edge in a partition of the graph. This function will
overwrites any existing edge.
*/
func (gt *concurrentTrans) StoreEdge(part string, edge data.Edge) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	return gt.Trans.StoreEdge(part, edge)
}

/*
RemoveEdge removes a single edge from a partition of the graph.
*/
func (gt *concurrentTrans) RemoveEdge(part string, ekey string, ekind string) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	return gt.Trans.RemoveEdge(part, ekey, ekind)
}

/*
rollingTrans is a rolling transaction which will commit itself after
n operations.
*/
type rollingTrans struct {
	id string   // ID of this transaction
	gm *Manager // Graph manager which created this transaction

	currentTrans Trans                     // Current transaction which is build up
	newTransFunc func(*Manager) Trans      // Function to create a new transaction
	transErrors  *errorutil.CompositeError // Collected transaction errors

	opThreshold   int             // Operation threshold
	opCount       int             // Operation count
	inFlightCount int             // Previous transactions which are still committing
	wg            *sync.WaitGroup // WaitGroup which releases after all in-flight transactions

	countNodeIns int // Count for inserted nodes
	countNodeRem int // Count for removed nodes
	countEdgeIns int // Count for inserted edges
	countEdgeRem int // Count for removed edges

	transLock *sync.RWMutex // Lock for this transaction
}

/*
ID returns a unique transaction ID.
*/
func (gt *rollingTrans) ID() string {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	return gt.id
}

/*
IsEmpty returns if this transaction is empty.
*/
func (gt *rollingTrans) IsEmpty() bool {
	sn, se, rn, re := gt.Counts()

	return sn == 0 && se == 0 && rn == 0 && re == 0
}

/*
Counts returns the transaction size in terms of objects. Returned values
are nodes to store, edges to store, nodes to remove and edges to remove.
*/
func (gt *rollingTrans) Counts() (int, int, int, int) {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	// Count current trans

	ns, es, nr, er := gt.currentTrans.Counts()

	return ns + gt.countNodeIns, es + gt.countEdgeIns,
		nr + gt.countNodeRem, er + gt.countEdgeRem
}

/*
String returns a string representation of this transatction.
*/
func (gt *rollingTrans) String() string {
	gt.transLock.RLock()
	defer gt.transLock.RUnlock()

	ns, es, nr, er := gt.currentTrans.Counts()

	return fmt.Sprintf("Rolling transaction %v - Nodes: I:%v R:%v - "+
		"Edges: I:%v R:%v - Threshold: %v - In-flight: %v",
		gt.id, ns+gt.countNodeIns, nr+gt.countNodeRem, es+gt.countEdgeIns,
		er+gt.countEdgeRem, gt.opThreshold, gt.inFlightCount)
}

/*
Commit writes the remaining operations of this rolling transaction to
the graph database.
*/
func (gt *rollingTrans) Commit() error {

	// Commit current transaction

	gt.transLock.Lock()

	if err := gt.currentTrans.Commit(); err != nil {
		gt.transErrors.Add(err)
	}

	gt.transLock.Unlock()

	// Wait for other transactions

	gt.wg.Wait()

	// Return any errors

	if gt.transErrors.HasErrors() {
		return gt.transErrors
	}

	return nil
}

/*
checkNewSubTrans checks if a new sub-transaction should be started.
*/
func (gt *rollingTrans) checkNewSubTrans() {

	if gt.opCount++; gt.opCount >= gt.opThreshold {

		// Reset the op counter

		gt.opCount = 0

		// Start a new transaction and add the counts to the overall counts

		cTrans := gt.currentTrans
		gt.currentTrans = gt.newTransFunc(gt.gm)

		ns, es, nr, er := cTrans.Counts()

		gt.countNodeIns += ns
		gt.countNodeRem += nr
		gt.countEdgeIns += es
		gt.countEdgeRem += er

		// Start go routine which commits the current transaction

		gt.wg.Add(1)       // Add to WaitGroup so we can wait for all in-flight transactions
		gt.inFlightCount++ // Count the new in-flight transaction

		go func() {
			defer gt.wg.Done()

			err := cTrans.Commit()

			gt.transLock.Lock()

			if err != nil {

				// Store errors

				gt.transErrors.Add(err)
			}

			// Reduce the counts (do this even if there were errors)

			gt.countNodeIns -= ns
			gt.countNodeRem -= nr
			gt.countEdgeIns -= es
			gt.countEdgeRem -= er

			gt.inFlightCount--

			gt.transLock.Unlock()

		}()
	}
}

/*
StoreNode stores a single node in a partition of the graph. This function will
overwrites any existing node.
*/
func (gt *rollingTrans) StoreNode(part string, node data.Node) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	err := gt.currentTrans.StoreNode(part, node)

	if err == nil {
		gt.checkNewSubTrans()
	}

	return err
}

/*
UpdateNode updates a single node in a partition of the graph. This function will
only update the given values of the node.
*/
func (gt *rollingTrans) UpdateNode(part string, node data.Node) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	err := gt.currentTrans.UpdateNode(part, node)

	if err == nil {
		gt.checkNewSubTrans()
	}

	return err
}

/*
RemoveNode removes a single node from a partition of the graph.
*/
func (gt *rollingTrans) RemoveNode(part string, nkey string, nkind string) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	err := gt.currentTrans.RemoveNode(part, nkey, nkind)

	if err == nil {
		gt.checkNewSubTrans()
	}

	return err
}

/*
StoreEdge stores a single edge in a partition of the graph. This function will
overwrites any existing edge.
*/
func (gt *rollingTrans) StoreEdge(part string, edge data.Edge) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	err := gt.currentTrans.StoreEdge(part, edge)

	if err == nil {
		gt.checkNewSubTrans()
	}

	return err
}

/*
RemoveEdge removes a single edge from a partition of the graph.
*/
func (gt *rollingTrans) RemoveEdge(part string, ekey string, ekind string) error {
	gt.transLock.Lock()
	defer gt.transLock.Unlock()

	err := gt.currentTrans.RemoveEdge(part, ekey, ekind)

	if err == nil {
		gt.checkNewSubTrans()
	}

	return err
}
