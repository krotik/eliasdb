EliasDB
=======
EliasDB is a graph based database using a key-value store for disk storage. It aims to provide most functionality out-of-the box without the need for any third party libraries.

Low level storage file
----------------------
On the lowest level (see eliasdb/storage/file) EliasDB defines a StorageFile which models a file on disk which can contain A LOT of data. The file contains fixed size records each with a unique id. On disk the logical StorageFile is split into multiple files each with a maximum file size of 10 Gigabyte. The id of a record is an unsigned 64 bit value and it contains by default 4096 bytes of data. This means by default a StorageFile can address 75,639,783.42 Petabyte (as 64-bit unsigned values can address 18446.74 PetaByte).

The StorageFile can be optionally created with a transaction management. The TransactionManager will keep a transaction log on disk which will be "flushed" to the actual datastore from time to time. This happens automatically an a restart. Records which are currently in a transaction or in the transaction log are also held in memory. Records can be involved in multiple transactions, there is a transaction counter on each record.

The StorageFile is used by requesting a record (Get()), then modifying or reading the record and finally releasing the record. Records are only written to disk if they have been modified. If the transaction management is used then the transaction is started on the first modification of a record and finished by calling Flush() on the StorageFile. If a transaction needs to be aborted then a call to Rollback() will discard all modified records since the last Flush() call.


Pagination and linked lists for managing data
---------------------------------------------
Above the StorageFile and its records is a page management system. The first record of a StorageFile serves as a header which stores information about lists of pages. Each record is viewed as a page with pointers to previous and next pages (records). Pages form a linked list through these pointers. The header is also used to store several "root" values. The lists can be traversed either by using the API of the header object or by using a PageCursor - a pointer object which can be moved forward and backwards.


Slots for storing data
----------------------------------
The lists of pages are used to manage data slots. A data slot has a unique id (location) and can store an arbitrary amount of data. There are two types of slots: physical slots and logical slots. 

Physical slots are allocated space in StorageFile records. The amount of data a physical slot can store depends on its initial allocation - writing beyond the boundaries would overwrite data in other slots.

Physical slots have a 4 byte header which stores the slot's allocated size and used (current) size. The allocated size value is a packed integer using a 2 bit multiplier in the beginning - using these packed values a slot can grow up to 138681822 bytes (138 MB). The space allocation is exact up to 17 kb and becomes more and more wasteful with increasing slot size. The current size is stored as a difference to the allocated size. The maximum difference between allocated and current space is 65534 bytes (65 kb).

Logical slots are nothing more than pointers to physical slots. The data stored in a logical slot is stored in the physical slot which it points to. If the content of a logical slot changes and grows beyond the allocated space of its physical slot then the associated physical slot is changed. The logical slot pointer is updated to the new physical slot. The unique id (location) of the logical slot does not change.


Locations
---------
To make addressing of physical slots more easy EliasDB stores the address of a single slot in a packed 64-bit value. The 8 byte value uses 6 bytes to address a record inside a storage file and 2 bytes to address a byte inside a record. Using this addressing convention we can address: (2^48 / 2 - 1) * 4096 = 5.76460752 * 10^17 which is around 512 petabyte.


SlotManagers
------------
The allocation and deallocation of slots is managed by two slot manager objects. Each of these managers employs a special submanager object which handles free slots. Using these, the slot managers will try to reuse previously deallocated space as much as possible to keep wasted space to a minimum.

The PhysicalSlotManager is in charge of storing, retrieving, allocation and deallocation of physical slots. The object provides 3 data methods which allow a client to insert, update and fetch data; one method to deallocate slots which are no longer needed and one method to flush changes to disk.

The LogicalSlotManager is in change of managing references to physical slots in logical slots. The object provides 4 data methods which allow a client to insert, force insert at a specific location, update and fetch data; one method to declare a slot free and one method to flush changes to disk.


StorageManager
--------------
The low level storage API is defined in an interface called storage.StorageManager. The interface defines methods to store, retrieve, update and delete a given object to and from the disk. There are 2 main implementations:

The DiskStorageManager controls the actual PhysicalSlotManager and LogicalSlotManager objects. It holds references to all involved files and ensures exclusive access to them through a generated lock file. The lockfile is checked and attempting to open another instance of the DiskStorageManager on the same files will result in an error. The DiskStorageManager is also responsible for marshalling given abstract objects into a binary form which can be written to physical slots.

The CachedDiskStorageManager is a cache wrapper for the DiskStorageManager. Its purpose is to intercept calls and to maintain a cache of stored objects. The cache is limited in size by the number of total objects it references. Once the cache is full it will forget the objects which have been requested the least.


HTree
-----
The key / value store functionality is provided by an HTree datastructure using Austin Appleby's MurmurHash3 as hashing algorithm. The default tree has 4 levels each with 256 possible children. A hash code for the tree has 32 bits each byte may be used as an index on each level of the tree. A node in the tree is either a page which holds pointers to children or a bucket which holds actual key / value pairs. Buckets may be found on all levels of the tree. A bucket can contain up to 8 elements before it is turned into a page unless it is located on the 4th level where it can contain any number of key / value pairs.


GraphManager
------------
The API to the actual graph database structure is provided by a GraphManager object. The object provides several methods to store and retrieve Nodes and Edges and various information about them. Nodes are like maps: storing attribute names and values. Each node in the database must have a unique key and a kind for data segregation. Edges are designed to be nodes with special attributes. Each edge has two "end" entries which are pointers to nodes. Each "end" has a role and an edge can be specified by its spec from each "end":

<ROLE of "source end"> <KIND of edge> <ROLE of "target end"> <KIND of "target end">

Using these specs it is possible to traverse the graph from one node to another. The specs are also called traversal specs since they describe a traversal from one node to another. Each end has furthermore a cascading and a cascading last flag attribute. If the cascading flag is set then all delete operations on the end will be cascaded to the other end. If additionally the cascading last flag is set then the other end is only deleted iff all other edges with the same spec to this node have been deleted (i.e. only the last removed edge with actually remove the node).

The GraphManager object stores its data in a graphstorage.Storage which is implemented as a DiskGraphStorage using a StorageManager and as a MemoryGraphStorage using just memory storage.

To further facilitate data segregation nodes and edges can be stored in different "partitions" of the database. By default the GraphManager will also maintain a full text search index which contains all data stored in the graph database. It is possible to search nodes by word, phrase or direct match.

The graph database also supports transactions which should be used to queue multiple change requests. Unless commit is called no actual changes are done to the datastore. An automatic rollback is done if an error occurs during the commit stage.


DistributedStorage
------------------
The distribution wrapper cluster.DistributedStorage provides a fully transparent abstraction layer to EliasDB's graphstorage.Storage to enable data distribution and replication. A new instance is created by wrapping an existing instance of graphstorage.Storage (e.g. graphstorage.MemoryGraphStorage or graphstorage.DiskGraphStorage). Multiple instances of cluster.DistributedStorage can be joined together via the network to form a cluster. All operations send to any of the wrapper instances are distributed to one or more cluster members. The cluster has a peer-to-peer character, all members are equal and there is no coordinator.

The code tries to be as transparent as possible and will only involve client code during serious errors. Conflicts are usually solved by a simple "last one wins" policy. Availability is more important than 100% consistency. Background tasks ensure eventual consistency. The clustering code is split into two main parts: cluster management and data distribution.

The cluster management code in cluster/manager provides client / server interfaces for the single cluster members. It provides automatic configuration distribution, communication security and failure detection. The cluster is secured by a shared secret string which is never directly transmitted via the network. A periodically housekeeping task is used to detect member failures and synchronizing member state.

The data distribution code manages the actual distribution and replication of data. Depending on the configured replication factor each stored datum is replicated to multiple members in the cluster. The cluster size may expand or shrink (if replication factor > 1). With a replication factor of n the cluster becomes inoperable when more than n-1 members fail. Data is synchronized between members using simple Lamport timestamps. The cluster only provides eventual consistency. Recovering members are not updated immediately and may deliver outdated results for some time.
