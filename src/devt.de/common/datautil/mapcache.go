/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package datautil contains general data handling objects and helper methods.

A map based cache object storing string->interface{}. It is possible to specify
a maximum size, which when reached causes the oldest entries to be removed.
It is also possible to set an expiry time for values which causes values which
are too old to be purged.
*/
package datautil

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

/*
MapCache datastructure.
*/
type MapCache struct {
	data    map[string]interface{} // Data for the cache
	ts      map[string]int64       // Timestamps for values
	size    uint64                 // Size of the cache
	maxsize uint64                 // Max size of the cache
	maxage  int64                  // Max age of the cache
	mutex   *sync.RWMutex          // Mutex to protect atomic map operations
}

/*
NewMapCache creates a new MapCache object. The calling function can specify
the maximum size and the maximum age in seconds for entries. A value of 0
means no size constraint and no age constraint.
*/
func NewMapCache(maxsize uint64, maxage int64) *MapCache {
	return &MapCache{make(map[string]interface{}), make(map[string]int64),
		0, maxsize, maxage, &sync.RWMutex{}}
}

/*
Put stores an item in the MapCache.
*/
func (mc *MapCache) Put(k string, v interface{}) {

	// Do cache maintenance

	oldest := mc.maintainCache()

	// Take writer lock

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Check if the entry is a new entry

	if _, exists := mc.data[k]; !exists {

		// If the list is full remove the oldest item otherwise increase the size

		if mc.maxsize != 0 && mc.size == mc.maxsize {
			delete(mc.data, oldest)
			delete(mc.ts, oldest)
		} else {
			mc.size++
		}
	}

	// Do the actual map operation

	mc.data[k] = v
	mc.ts[k] = time.Now().Unix()
}

/*
Remove removes an item in the MapCache.
*/
func (mc *MapCache) Remove(k string) bool {

	// Do cache maintenance

	mc.maintainCache()

	// Take writer lock

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Check if the entry exists

	_, exists := mc.data[k]

	if exists {

		// Do the actual map operation

		delete(mc.data, k)
		delete(mc.ts, k)

		mc.size--
	}

	return exists
}

/*
Get retrieves an item from the MapCache.
*/
func (mc *MapCache) Get(k string) (interface{}, bool) {

	// Do cache maintenance

	mc.maintainCache()

	// Take reader lock

	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	// Do the actual map operation

	v, ok := mc.data[k]

	return v, ok
}

/*
String returns a string representation of this MapCache.
*/
func (mc *MapCache) String() string {

	// Sort keys before printing the map

	var keys []string
	for k := range mc.data {
		keys = append(keys, k)
	}
	sort.Sort(sort.StringSlice(keys))

	buf := &bytes.Buffer{}
	for _, k := range keys {
		buf.WriteString(fmt.Sprint(k, ":", mc.data[k], "\n"))
	}

	return buf.String()
}

/*
maintainCache removes expired items and returns the oldest entry.
*/
func (mc *MapCache) maintainCache() string {

	mc.mutex.RLock()

	var oldestTS int64 = math.MaxInt64
	oldestK := ""

	now := time.Now().Unix()

	for k, v := range mc.ts {

		// Check if the entry has expired

		if mc.maxage != 0 && now-v > mc.maxage {

			// Remove entry if it has expired

			mc.mutex.RUnlock()
			mc.mutex.Lock()

			delete(mc.data, k)
			delete(mc.ts, k)
			mc.size--

			mc.mutex.Unlock()
			mc.mutex.RLock()
		}

		// Gather oldest entry

		if v < oldestTS {
			oldestTS = v
			oldestK = k
		}
	}

	mc.mutex.RUnlock()

	return oldestK
}
