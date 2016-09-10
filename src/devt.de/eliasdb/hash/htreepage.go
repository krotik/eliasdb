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
)

/*
htreePage data structure
*/
type htreePage struct {
	*htreeNode
}

/*
newHTreePage creates a new page for the HTree.
*/
func newHTreePage(tree *HTree, depth byte) *htreePage {
	return &htreePage{&htreeNode{tree, 0, nil, depth, make([]uint64, MaxPageChildren), nil, nil, 0}}
}

/*
IsEmpty returns if this page is empty.
*/
func (p *htreePage) IsEmpty() bool {
	for _, child := range p.Children {
		if child != 0 {
			return false
		}
	}

	return true
}

/*
Location returns the location of this HTree page.
*/
func (p *htreePage) Location() uint64 {
	return p.loc
}

/*
Get gets a value for a given key.
*/
func (p *htreePage) Get(key []byte) (interface{}, *htreeBucket, error) {
	hash := p.hashKey(key)
	loc := p.Children[hash]

	if loc != 0 {
		node, err := p.fetchNode(loc)
		if err != nil {
			return nil, nil, err
		}

		if node.Children != nil {

			// If another page was found deligate the request

			page := &htreePage{node}

			page.loc = loc
			page.sm = p.sm

			return page.Get(key)

		}

		// If a Bucket was found return the value

		bucket := &htreeBucket{node}

		bucket.loc = loc
		bucket.sm = p.sm

		return bucket.Get(key), bucket, nil
	}

	return nil, nil, nil
}

/*
Exists checks if an element exists.
*/
func (p *htreePage) Exists(key []byte) (bool, error) {
	hash := p.hashKey(key)
	loc := p.Children[hash]

	if loc != 0 {
		node, err := p.fetchNode(loc)
		if err != nil {
			return false, err
		}

		if node.Children != nil {

			// If another page was found deligate the request

			page := &htreePage{node}

			page.loc = loc
			page.sm = p.sm

			return page.Exists(key)

		}

		// If a Bucket was found return the value

		bucket := &htreeBucket{node}

		return bucket.Exists(key), nil
	}

	return false, nil
}

/*
Put adds or updates a new key / value pair.
*/
func (p *htreePage) Put(key []byte, value interface{}) (interface{}, error) {

	// Putting a nil values will remove the element

	if value == nil {
		return p.Remove(key)
	}

	hash := p.hashKey(key)
	loc := p.Children[hash]

	if loc == 0 {

		// If nothing exists yet for the hash code then create a new bucket

		bucket := newHTreeBucket(p.tree, p.Depth+1)

		existing := bucket.Put(key, value)

		loc, err := p.sm.Insert(bucket.htreeNode)
		if err != nil {
			return nil, err
		}

		bucket.loc = loc
		bucket.sm = p.sm

		p.Children[hash] = loc

		err = p.sm.Update(p.loc, p.htreeNode)
		if err != nil {
			return nil, err
		}

		return existing, nil

	}
	// If a bucket was found try to put the value on it if there is room

	node, err := p.fetchNode(loc)
	if err != nil {
		return false, err
	}

	if node.Children != nil {

		// If another page was found deligate the request

		page := &htreePage{node}

		page.loc = loc
		page.sm = p.sm

		return page.Put(key, value)

	}

	// If a bucket was found try to put the value on it if there is room

	bucket := &htreeBucket{node}

	bucket.loc = loc
	bucket.sm = p.sm

	if bucket.HasRoom() {

		existing := bucket.Put(key, value)

		return existing, p.sm.Update(bucket.loc, bucket.htreeNode)

	}

	// If the bucket is too full create a new directory

	if p.Depth == MaxTreeDepth {
		panic("Max depth of HTree exceeded")
	}

	page := newHTreePage(p.tree, p.Depth+1)

	ploc, err := p.sm.Insert(page.htreeNode)
	if err != nil {
		return nil, err
	}

	page.loc = ploc
	page.sm = p.sm

	p.Children[hash] = ploc

	if err := p.sm.Update(p.loc, p.htreeNode); err != nil {

		// Try to clean up

		p.Children[hash] = loc
		p.sm.Free(ploc)

		return nil, err
	}

	// At this point the bucket has been removed from the list of children
	// It is no longer part of the tree

	// Try inserting all keys of the bucket into the newly created page
	// and remove the bucket - no error checking here - the recovery
	// steps are too eloborate with little chance of success they
	// might also damage the now intact tree

	for i, key := range bucket.Keys {
		page.Put(key, bucket.Values[i])
	}

	// Remove old bucket from file

	p.sm.Free(bucket.loc)

	// Finally insert key / value pair

	return page.Put(key, value)
}

/*
Remove removes a key / value pair.
*/
func (p *htreePage) Remove(key []byte) (interface{}, error) {
	hash := p.hashKey(key)
	loc := p.Children[hash]

	// Return if there is nothing to delete

	if loc == 0 {
		return nil, nil
	}

	node, err := p.fetchNode(loc)
	if err != nil {
		return false, err
	}

	if node.Children != nil {

		// If another page was found deligate the request

		page := &htreePage{node}

		page.loc = loc
		page.sm = p.sm

		ret, err := page.Remove(key)
		if err != nil {
			return ret, err
		}

		if page.IsEmpty() {

			// Remove page if it is empty

			p.Children[hash] = 0

			if err := p.sm.Update(p.loc, p.htreeNode); err != nil {
				return nil, err
			}

			return ret, p.sm.Free(loc)
		}

		return ret, nil

	}

	// If a bucket is found just remove the key / value pair

	bucket := &htreeBucket{node}

	bucket.loc = loc
	bucket.sm = p.sm

	ret := bucket.Remove(key)

	// Either update or remove the bucket

	if bucket.Size() > 0 {
		return ret, p.sm.Update(bucket.loc, bucket.htreeNode)
	}

	p.Children[hash] = 0

	if err := p.sm.Update(p.loc, p.htreeNode); err != nil {
		return nil, err
	}

	return ret, p.sm.Free(loc)
}

/*
String returns a string representation of this page.
*/
func (p *htreePage) String() string {
	var j byte
	buf := new(bytes.Buffer)

	for j = 0; j < p.Depth; j++ {
		buf.WriteString("  ")
	}
	buf.WriteString(fmt.Sprintf("HashPage %v (depth: %v)\n", p.loc, p.Depth))

	for hash, child := range p.Children {

		if child != 0 {

			for j = 0; j < p.Depth+1; j++ {
				buf.WriteString("  ")
			}
			buf.WriteString(fmt.Sprintf("Hash %08X (loc: %v)\n", hash, child))

			node, err := p.fetchNode(child)
			if err != nil {

				buf.WriteString(err.Error())
				buf.WriteString("\n")

			} else if node.Children != nil {

				page := &htreePage{node}

				page.loc = child
				page.sm = p.sm

				buf.WriteString(page.String())

			} else {

				bucket := &htreeBucket{node}

				buf.WriteString(bucket.String())
			}
		}
	}

	return buf.String()
}

/*
hashKey calculates the hash code for a given key.
*/
func (p *htreePage) hashKey(key []byte) uint32 {
	var hash, hashMask uint32

	// Calculate mask depending on page depth
	// 0 masks out most significant bits while 2 masks out least significant bits

	hashMask = (MaxPageChildren - 1) << ((MaxTreeDepth - p.Depth) * PageLevelBits)

	// Calculate hash and apply mask

	hash, _ = MurMurHashData(key, 0, len(key)-1, 42)
	hash = hash & hashMask

	// Move the bytes to the least significant position

	hash = hash >> ((MaxTreeDepth - p.Depth) * PageLevelBits)

	return hash % MaxPageChildren
}
