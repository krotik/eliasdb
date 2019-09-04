/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package hash

import (
	"bytes"
	"fmt"

	"devt.de/krotik/common/stringutil"
)

/*
htreeBucket data structure
*/
type htreeBucket struct {
	*htreeNode
}

/*
htreeBucket creates a new bucket for the HTree.
*/
func newHTreeBucket(tree *HTree, depth byte) *htreeBucket {
	return &htreeBucket{&htreeNode{tree, 0, nil, depth, nil,
		make([][]byte, MaxBucketElements),
		make([]interface{}, MaxBucketElements), 0}}
}

/*
Size returns the size of this bucket.
*/
func (b *htreeBucket) Size() byte {
	return b.BucketSize
}

/*
IsLeaf returns if this bucket is a leaf node.
*/
func (b *htreeBucket) IsLeaf() bool {
	return b.Depth == MaxTreeDepth+1
}

/*
HasRoom returns if this bucket has room for more data.
*/
func (b *htreeBucket) HasRoom() bool {
	if b.IsLeaf() {
		return true
	}
	return b.BucketSize < MaxBucketElements
}

/*
Put adds or updates a new key / value pair to the bucket.
*/
func (b *htreeBucket) Put(key []byte, value interface{}) interface{} {
	if key == nil {
		return nil
	}

	// Check if this is an update

	for i, skey := range b.Keys {

		if bytes.Compare(key, skey) == 0 {
			old := b.Values[i]
			b.Values[i] = value

			return old
		}
	}

	if !b.HasRoom() {
		panic("Bucket has no more room")
	}

	if b.BucketSize >= MaxBucketElements {
		b.Keys = append(b.Keys, key)
		b.Values = append(b.Values, value)
		b.BucketSize++
		return nil
	}

	b.Keys[b.BucketSize] = key
	b.Values[b.BucketSize] = value
	b.BucketSize++

	return nil
}

/*
Remove removes a key / value pair from the bucket.
*/
func (b *htreeBucket) Remove(key []byte) interface{} {
	if key == nil || b.BucketSize == 0 {
		return nil
	}

	// Look for the key

	for i, skey := range b.Keys {

		if bytes.Compare(key, skey) == 0 {
			old := b.Values[i]

			b.Keys[i] = b.Keys[b.BucketSize-1]
			b.Values[i] = b.Values[b.BucketSize-1]

			b.Keys[b.BucketSize-1] = nil
			b.Values[b.BucketSize-1] = nil

			b.BucketSize--

			return old
		}
	}

	return nil
}

/*
Get gets the value for a given key.
*/
func (b *htreeBucket) Get(key []byte) interface{} {
	if key == nil || b.BucketSize == 0 {
		return nil
	}

	// Look for the key

	for i, skey := range b.Keys {

		if bytes.Compare(key, skey) == 0 {
			return b.Values[i]
		}
	}

	return nil
}

/*
Exists checks if an element exists.
*/
func (b *htreeBucket) Exists(key []byte) bool {
	if key == nil || b.BucketSize == 0 {
		return false
	}

	// Look for the key

	for _, skey := range b.Keys {

		if bytes.Compare(key, skey) == 0 {
			return true
		}
	}

	return false
}

/*
String returns a string representation of this bucket.
*/
func (b *htreeBucket) String() string {
	var j byte
	buf := new(bytes.Buffer)

	for j = 0; j < b.Depth; j++ {
		buf.WriteString("  ")
	}
	buf.WriteString(fmt.Sprintf("HashBucket (%v element%s, depth: %v)\n",
		b.Size(), stringutil.Plural(int(b.Size())), b.Depth))

	for i, key := range b.Keys {

		for j = 0; j < b.Depth; j++ {
			buf.WriteString("  ")
		}
		buf.WriteString(fmt.Sprintf("%v - %v\n", key, b.Values[i]))
	}

	return buf.String()
}
