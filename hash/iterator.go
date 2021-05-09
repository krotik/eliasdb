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
	"errors"
	"fmt"

	"devt.de/krotik/eliasdb/storage"
)

/*
ErrNoMoreItems is assigned to LastError when Next() is called and there are no
more items to iterate.
*/
var ErrNoMoreItems = errors.New("No more items to iterate")

/*
HTreeIterator data structure
*/
type HTreeIterator struct {
	tree      *HTree      // Tree to iterate
	nodePath  []uint64    // Path in the tree we currently traversing
	indices   []int       // List of the current indices in the current path
	nextKey   []byte      // Next iterator key (overwritten by nextItem)
	nextValue interface{} // Next iterator value
	LastError error       // Last encountered error
}

/*
NewHTreeIterator creates a new HTreeIterator.
*/
func NewHTreeIterator(tree *HTree) *HTreeIterator {
	it := &HTreeIterator{tree, make([]uint64, 0), make([]int, 0), nil, nil, nil}

	it.nodePath = append(it.nodePath, tree.Root.Location())
	it.indices = append(it.indices, -1)

	// Set the nextKey and nextValue properties

	it.Next()

	return it
}

/*
HasNext returns if there is a next key / value pair.
*/
func (it *HTreeIterator) HasNext() bool {
	return it.nextKey != nil
}

/*
Next returns the next key / value pair.
*/
func (it *HTreeIterator) Next() ([]byte, interface{}) {
	key := it.nextKey
	value := it.nextValue

	if err := it.nextItem(); err != ErrNoMoreItems && err != nil {

		it.LastError = err

		// There was a serious error terminate the iterator

		it.nodePath = make([]uint64, 0)
		it.indices = make([]int, 0)
		it.nextKey = nil
		it.nextValue = nil
	}

	return key, value
}

/*
Retrieve the next key / value pair for the iterator. The tree might
have changed significantly after the last call. We need to cope
with errors as best as we can.
*/
func (it *HTreeIterator) nextItem() error {

	// Check if there are more items available to iterate

	if len(it.nodePath) == 0 {
		it.nextKey = nil
		it.nextValue = nil

		return ErrNoMoreItems
	}

	// Get the current path element

	loc := it.nodePath[len(it.nodePath)-1]
	index := it.indices[len(it.indices)-1]

	node, err := it.tree.Root.fetchNode(loc)

	if err != nil {

		if smr, ok := err.(*storage.ManagerError); ok && smr.Type == storage.ErrSlotNotFound {

			// Something is wrong - the tree must have changed since the last
			// nextItem call. Remove the path element and try again.

			it.nodePath = it.nodePath[:len(it.nodePath)-1]
			it.indices = it.indices[:len(it.indices)-1]

			return it.nextItem()
		}

		// If it is another error there is something more serious - report it

		return err
	}

	if node.Children != nil {

		// If the current path element is a page get the next child and delegate

		page := &htreePage{node}

		page.loc = loc
		page.sm = it.tree.Root.sm

		nextChild := it.searchNextChild(page, index)

		if nextChild != -1 {

			// If we found another element then update the current index  and delegate to it

			it.indices[len(it.indices)-1] = nextChild

			it.nodePath = append(it.nodePath, page.Children[nextChild])
			it.indices = append(it.indices, -1)

			return it.nextItem()

		}

		// If we finished this page remove it from the stack and continue
		// with the parent

		it.nodePath = it.nodePath[:len(it.nodePath)-1]
		it.indices = it.indices[:len(it.indices)-1]

		return it.nextItem()
	}

	// If the current path element is a bucket just iterate the elements
	// delegate once it has finished

	bucket := &htreeBucket{node}

	bucket.loc = loc
	bucket.sm = it.tree.Root.sm

	nextElement := it.searchNextElement(bucket, index)

	if nextElement != -1 {

		// If we found another element then update the current index and return it

		it.indices[len(it.indices)-1] = nextElement

		it.nextKey = bucket.Keys[nextElement]
		it.nextValue = bucket.Values[nextElement]

		return nil
	}

	// If we finished this bucket remove it from the stack and continue
	// with the parent

	it.nodePath = it.nodePath[:len(it.nodePath)-1]
	it.indices = it.indices[:len(it.indices)-1]

	return it.nextItem()
}

/*
searchNextChild searches for the index of the next available page child from a given index.
*/
func (it *HTreeIterator) searchNextChild(page *htreePage, current int) int {
	for i := current + 1; i < MaxPageChildren; i++ {
		child := page.Children[i]

		if child != 0 {
			return i
		}
	}

	return -1
}

/*
searchNextElement searches for the index of the next available bucket element from a given index.
*/
func (it *HTreeIterator) searchNextElement(bucket *htreeBucket, current int) int {
	next := current + 1

	if next < int(bucket.BucketSize) {
		return next
	}

	return -1
}

/*
Return a string representation of the iterator.
*/
func (it *HTreeIterator) String() string {
	return fmt.Sprintf("HTree Iterator (tree: %v)\n  path: %v\n  indices: %v\n  next: %v / %v\n",
		it.tree.Root.Location(), it.nodePath, it.indices, it.nextKey, it.nextValue)
}
