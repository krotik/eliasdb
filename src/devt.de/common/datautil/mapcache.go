/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
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
MapCache is a map based cache object storing string->interface{}. It is possible
to specify a maximum size, which when reached causes the oldest entries to be
removed. It is also possible to set an expiry time for values. Values which are
old are purged on the next access to the object.
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
Clear removes all entries.
*/
func (mc *MapCache) Clear() {

	// Take writer lock

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.data = make(map[string]interface{})
	mc.ts = make(map[string]int64)

	mc.size = 0
}

/*
Size returns the current size of the MapCache.
*/
func (mc *MapCache) Size() uint64 {
	return mc.size
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
GetAll retrieves all items from the MapCache.
*/
func (mc *MapCache) GetAll() map[string]interface{} {

	// Do cache maintenance

	mc.maintainCache()

	// Take reader lock

	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	// Create return map

	cp := make(map[string]interface{})

	for k, v := range mc.data {
		cp[k] = v
	}

	return cp
}

/*
String returns a string representation of this MapCache.
*/
func (mc *MapCache) String() string {

	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

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

	oldestTS := int64(math.MaxInt64)
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
