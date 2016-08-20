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

This file contains constants.
*/
package graph

/*
VERSION of the GraphManager
*/
const VERSION = 1

/*
MainDBEntryPrefix is the prefix for entries stored in the main database
*/
const MainDBEntryPrefix = string(0x2)

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
const PrefixNSAttrs = string(0x01)

/*
PrefixNSAttr is the prefix for storing the value of a node attribute
*/
const PrefixNSAttr = string(0x02)

/*
PrefixNSSpecs is the prefix for storing specs of edges related to a node
*/
const PrefixNSSpecs = string(0x03)

/*
PrefixNSEdge is the prefix for storing a link from a node (and a spec) to an edge
*/
const PrefixNSEdge = string(0x04)

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
