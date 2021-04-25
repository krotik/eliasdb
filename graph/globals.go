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
Package graph contains the main API to the graph datastore.

Manager API

The main API is provided by a Manager object which can be created with the
NewGraphManager() constructor function. The manager CRUD functionality for
nodes and edges through store, fetch and remove functions. It also provides
the basic traversal functionality which allos the traversal from one node to
other nodes.

Node iterator

All available node keys in a partition of a given kind can be iterated by using
a NodeKeyIterator. The manager can produce these with the NodeKeyIterator()
function.

Fulltext search

All nodes and edges in the datastore are indexed. The index can be queried
using a IndexQuery object. The manager can produce these with the NodeIndexQuery()
or EdgeIndexQuery function.

Transactions

A transaction is used to build up multiple store and delete tasks for the
graph database. Nothing is written to the database before calling commit().
A transaction commit does an automatic rollback if an error occurs
(except fatal disk write errors which might cause a panic).

A trans object can be created with the NewGraphTrans() function.

Rules

(Use with caution)

Graph rules provide automatic operations which help to keep the graph consistent.
Rules trigger on global graph events. The rules SystemRuleDeleteNodeEdges and
SystemRuleUpdateNodeStats are automatically loaded when a new Manager is created.
See the code for further details.

Graph databases

A graph manager handles the graph storage and provides the API for
the graph database. The storage is divided into several databases:

Main database

MainDB stores various meta information such as known node/edge kinds, attributes
or version information.

Names database

Names can be encoded (into a number) or decoded (into a string)

	32 bit values for any given node attribute names
	16 bit values for any given edge role names
	16 bit values for any given edge kind names

Nodes database

Each node kind database stores:

	PrefixNSAttrs + node key -> [ ATTRS ]
	(a list of attributes of a certain node)

	PrefixNSAttr + node key + attr num -> value
	(attribute value of a certain node)

	PrefixNSSpecs + node key -> map[spec]<empty string>
	(a lookup for available specs for a certain node)

	PrefixNSEdge + node key + spec -> map[edge key]edgeinfo{other node key, other node kind}]
	(connection from one node to another via a spec)

Edges database

Each edge kind database stores:

	PrefixNSAttrs + edge key -> [ ATTRS ]
	(a list of attributes of a certain edge)

	PrefixNSAttr + edge key + attr num -> value
	(attribute value of a certain edge)

Index database

The text index managed by util/indexmanager.go. IndexQuery provides access to
the full text search index.
*/
package graph

/*
VERSION of the GraphManager
*/
const VERSION = 1

/*
MainDBEntryPrefix is the prefix for entries stored in the main database
*/
const MainDBEntryPrefix = "\x02"

// MainDB entries
// ==============

/*
MainDBVersion is the MainDB entry key for version information
*/
const MainDBVersion = MainDBEntryPrefix + "ver"

/*
MainDBNodeKinds is the MainDB entry key for node kind information
*/
const MainDBNodeKinds = MainDBEntryPrefix + "nodekind"

/*
MainDBEdgeKinds is the MainDB entry key for edge kind information
*/
const MainDBEdgeKinds = MainDBEntryPrefix + "edgekind"

/*
MainDBParts is the MainDB entry key for partition information
*/
const MainDBParts = MainDBEntryPrefix + "part"

/*
MainDBNodeAttrs is the MainDB entry key for a list of node attributes
*/
const MainDBNodeAttrs = MainDBEntryPrefix + "natt"

/*
MainDBNodeEdges is the MainDB entry key for a list of node relationships
*/
const MainDBNodeEdges = MainDBEntryPrefix + "nrel"

/*
MainDBNodeCount is the MainDB entry key for a node count
*/
const MainDBNodeCount = MainDBEntryPrefix + "ncnt"

/*
MainDBEdgeAttrs is the MainDB entry key for a list of edge attributes
*/
const MainDBEdgeAttrs = MainDBEntryPrefix + "eatt"

/*
MainDBEdgeCount is the MainDB entry key for an edge count
*/
const MainDBEdgeCount = MainDBEntryPrefix + "ecnt"

// Root IDs for StorageManagers
// ============================

/*
RootIDNodeHTree is the root ID for the HTree holding primary information
*/
const RootIDNodeHTree = 2

/*
RootIDNodeHTreeSecond is the root ID for the HTree holding secondary information
*/
const RootIDNodeHTreeSecond = 3

// Suffixes for StorageManagers
// ============================

/*
StorageSuffixNodes is the suffix for a node storage
*/
const StorageSuffixNodes = ".nodes"

/*
StorageSuffixNodesIndex is the suffix for a node index
*/
const StorageSuffixNodesIndex = ".nodeidx"

/*
StorageSuffixEdges is the suffix for an edge storage
*/
const StorageSuffixEdges = ".edges"

/*
StorageSuffixEdgesIndex is the suffix for an edge index
*/
const StorageSuffixEdgesIndex = ".edgeidx"

// PREFIXES for Node storage
// =========================

// Prefixes are only one byte. They should be followed by the node key so
// similar entries are stored near each other physically.
//

/*
PrefixNSAttrs is the prefix for storing attributes of a node
*/
const PrefixNSAttrs = "\x01"

/*
PrefixNSAttr is the prefix for storing the value of a node attribute
*/
const PrefixNSAttr = "\x02"

/*
PrefixNSSpecs is the prefix for storing specs of edges related to a node
*/
const PrefixNSSpecs = "\x03"

/*
PrefixNSEdge is the prefix for storing a link from a node (and a spec) to an edge
*/
const PrefixNSEdge = "\x04"

// Graph events
//=============

/*
EventNodeCreated is thrown when a node gets created.

Parameters: partition of created node, created node
*/
const EventNodeCreated = 0x01

/*
EventNodeUpdated is thrown when a node gets updated.

Parameters: partition of updated node, updated node, old node
*/
const EventNodeUpdated = 0x02

/*
EventNodeDeleted is thrown when a node gets deleted.

Parameters: partition of deleted node, deleted node
*/
const EventNodeDeleted = 0x03

/*
EventEdgeCreated is thrown when an edge gets created.

Parameters: partition of created edge, created edge
*/
const EventEdgeCreated = 0x04

/*
EventEdgeUpdated is thrown when an edge gets updated.

Parameters: partition of updated edge, updated edge, old edge
*/
const EventEdgeUpdated = 0x05

/*
EventEdgeDeleted is thrown when an edge gets deleted.

Parameters: partition of deleted edge, deleted edge
*/
const EventEdgeDeleted = 0x06
