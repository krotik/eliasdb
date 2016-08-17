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

/*
GraphManager Version
*/
const VERSION = 1

/*
Prefix for entries stored in the main database
*/
const MAINDB_ENTRY_PREFIX = string(0x2)

// MainDB entries
// ==============

/*
MainDB entry key for version information
*/
const MAINDB_VERSION = MAINDB_ENTRY_PREFIX + "ver"

/*
MainDB entry key for node kind information
*/
const MAINDB_NODE_KINDS = MAINDB_ENTRY_PREFIX + "nodekind"

/*
MainDB entry key for edge kind information
*/
const MAINDB_EDGE_KINDS = MAINDB_ENTRY_PREFIX + "edgekind"

/*
MainDB entry key for partition information
*/
const MAINDB_PARTS = MAINDB_ENTRY_PREFIX + "part"

/*
MainDB entry key for a list of node attributes
*/
const MAINDB_NODE_ATTRS = MAINDB_ENTRY_PREFIX + "natt"

/*
MainDB entry key for a list of node relationships
*/
const MAINDB_NODE_EDGES = MAINDB_ENTRY_PREFIX + "nrel"

/*
MainDB entry key for a node count
*/
const MAINDB_NODE_COUNT = MAINDB_ENTRY_PREFIX + "ncnt"

/*
MainDB entry key for a list of edge attributes
*/
const MAINDB_EDGE_ATTRS = MAINDB_ENTRY_PREFIX + "eatt"

/*
MainDB entry key for an edge count
*/
const MAINDB_EDGE_COUNT = MAINDB_ENTRY_PREFIX + "ecnt"

// Root IDs for StorageManagers
// ============================

/*
Root id for HTree slot
*/
const ROOT_ID_NODE_HTREE = 2

/*
Root id for second HTree slot
*/
const ROOT_ID_NODE_HTREE_SECOND = 3

// Suffixes for StorageManagers
// ============================

/*
Suffix for a node storage
*/
const STORAGE_SUFFIX_NODES = ".nodes"

/*
Suffix for a node index
*/
const STORAGE_SUFFIX_NODES_INDEX = ".nodeidx"

/*
Suffix for an edge storage
*/
const STORAGE_SUFFIX_EDGES = ".edges"

/*
Suffix for an edge index
*/
const STORAGE_SUFFIX_EDGES_INDEX = ".edgeidx"

// PREFIXES for Node storage
// =========================

// Prefixes are only one byte. They should be followed by the node key so
// similar entries are stored near each other physically.
//

/*
Prefix for storing attributes of a node
*/
const PREFIX_NS_ATTRS = string(0x01)

/*
Prefix for storing the value of a node attribute
*/
const PREFIX_NS_ATTR = string(0x02)

/*
Prefix for storing specs of edges related to a node
*/
const PREFIX_NS_SPECS = string(0x03)

/*
Prefix for storing link from a node (and a spec) to an edge
*/
const PREFIX_NS_EDGE = string(0x04)

// Graph events
//=============

/*
Thrown when a node gets created.

Parameters: partition of created node, created node
*/
const EVENT_NODE_CREATED = 0x01

/*
Thrown when a node gets updated.

Parameters: partition of updated node, updated node, old node
*/
const EVENT_NODE_UPDATED = 0x02

/*
Thrown when a node gets deleted.

Parameters: partition of deleted node, deleted node
*/
const EVENT_NODE_DELETED = 0x03

/*
Thrown when an edge gets created.

Parameters: partition of created edge, created edge
*/
const EVENT_EDGE_CREATED = 0x04

/*
Thrown when an edge gets updated.

Parameters: partition of updated edge, updated edge, old edge
*/
const EVENT_EDGE_UPDATED = 0x05

/*
Thrown when an edge gets deleted.

Parameters: partition of deleted edge, deleted edge
*/
const EVENT_EDGE_DELETED = 0x06
