/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package util

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"devt.de/common/bitutil"
	"devt.de/common/sortutil"
	"devt.de/common/stringutil"
	"devt.de/eliasdb/hash"
)

/*
MaxKeysetSize is the maximum number of keys for a single word lookup.
*/
const MaxKeysetSize = 1000

/*
CaseSensitiveWordIndex is a flag to indicate if the index should be case sensitive.
*/
var CaseSensitiveWordIndex = false

/*
PrefixAttrWord is the prefix used for word index entries
*/
const PrefixAttrWord = string(0x01)

/*
PrefixAttrHash is the prefix used for hashes of attribute values
*/
const PrefixAttrHash = string(0x02)

/*
IndexManager data structure
*/
type IndexManager struct {
	htree *hash.HTree // Persistent HTree which stores this index
}

/*
indexEntry data structure
*/
type indexEntry struct {
	WordPos map[string]string // Node id to word position array
}

func init() {

	// Make sure we can use indexEntry in a gob operation

	gob.Register(&indexEntry{})
}

/*
NewIndexManager creates a new index manager instance.
*/
func NewIndexManager(htree *hash.HTree) *IndexManager {
	return &IndexManager{htree}
}

/*
Index indexes (inserts) a given object.
*/
func (im *IndexManager) Index(key string, obj map[string]string) error {
	return im.updateIndex(key, obj, nil)
}

/*
Reindex reindexes (updates) a given object.
*/
func (im *IndexManager) Reindex(key string, newObj map[string]string,
	oldObj map[string]string) error {

	return im.updateIndex(key, newObj, oldObj)
}

/*
Deindex deindexes (removes) a given object.
*/
func (im *IndexManager) Deindex(key string, obj map[string]string) error {
	return im.updateIndex(key, nil, obj)
}

/*
LookupPhrase finds all nodes where an attribute contains a certain phrase. This
call returns a list of node keys which contain the phrase at least once.
*/
func (im *IndexManager) LookupPhrase(attr, phrase string) ([]string, error) {

	// Chop up the phrase into words

	phraseWords := strings.FieldsFunc(phrase, func(r rune) bool {
		return !stringutil.IsAlphaNumeric(string(r)) && (unicode.IsSpace(r) || unicode.IsControl(r) || unicode.IsPunct(r))
	})

	// Lookup every phrase word

	results := make([]map[string][]uint64, len(phraseWords))

	for i, phraseWord := range phraseWords {

		res, err := im.LookupWord(attr, phraseWord)
		if err != nil {
			return nil, &GraphError{ErrIndexError, err.Error()}
		}

		results[i] = res
	}

	if len(results) == 0 || len(results[0]) == 0 {
		return nil, nil
	}

	ret := make([]string, 0, len(results[0]))

	// Go through all found nodes and try to find a path

	path := make([]uint64, 0, len(phraseWords))

	for key := range results[0] {

		path = path[:0]

		foundWords := im.findPhrasePath(key, 0, path, phraseWords, results)

		if foundWords == len(phraseWords) {

			// Add key to results if a path was found

			ret = append(ret, key)
		}
	}

	// Guarantee a stable result

	sort.StringSlice(ret).Sort()

	return ret, nil
}

/*
findPhrasePath tries to find a phrase in a given set of lookup results.
*/
func (im *IndexManager) findPhrasePath(key string, index int, path []uint64,
	phraseWords []string, results []map[string][]uint64) int {

	// Get the results for this word index

	result := results[index]

	// Check if there is a result for the given key

	if posArr, ok := result[key]; ok {

		// Check if any of the positions is at the right place

		if index > 0 {

			// Check with previous result

			for _, pos := range posArr {

				// Check if the position array contains the expected next word position

				if pos == path[index-1]+1 {
					path = append(path, pos)
					break
				}

				// Abort if the expected position cannot be there

				if pos > path[index-1] {
					return len(path)
				}

			}

			// Do the next iteration if a position was found and
			// there are more words in the phrase to match

			if len(path) == index+1 && index < len(phraseWords)-1 {
				return im.findPhrasePath(key, index+1, path, phraseWords, results)
			}

			return index + 1

		}

		// Try every position as start position in the first iteration

		for _, pos := range posArr {

			path = path[:0]
			path = append(path, pos)

			// Test if the phrase only contained one word

			if len(phraseWords) == 1 {
				return 1
			}

			// Find the rest

			ret := im.findPhrasePath(key, 1, path, phraseWords, results)

			if ret == len(phraseWords) {
				return ret
			}
		}
	}

	return len(path)
}

/*
LookupWord finds all nodes where an attribute contains a certain word. This call returns
a map which maps node key to a list of word positions.
*/
func (im *IndexManager) LookupWord(attr, word string) (map[string][]uint64, error) {
	var s string

	if CaseSensitiveWordIndex {
		s = word
	} else {
		s = strings.ToLower(word)
	}

	entry, err := im.htree.Get([]byte(PrefixAttrWord + attr + s))

	if err != nil {
		return nil, &GraphError{ErrIndexError, err.Error()}
	} else if entry == nil {
		return nil, nil
	}

	ret := make(map[string][]uint64)

	for k, l := range entry.(*indexEntry).WordPos {
		ret[k] = bitutil.UnpackList(l)
	}

	return ret, nil
}

/*
LookupValue finds all nodes where an attribute has a certain value. This call
returns a list of node keys.
*/
func (im *IndexManager) LookupValue(attr, value string) ([]string, error) {
	var entry *indexEntry
	var sum [16]byte

	if CaseSensitiveWordIndex {
		sum = md5.Sum([]byte(value))
	} else {
		sum = md5.Sum([]byte(strings.ToLower(value)))
	}

	indexkey := []byte(PrefixAttrHash + attr + string(sum[:16]))

	// Retrieve index entry

	obj, err := im.htree.Get(indexkey)

	if err != nil {
		return nil, &GraphError{ErrIndexError, err.Error()}
	}

	if obj == nil {
		return nil, nil
	}

	entry = obj.(*indexEntry)

	ret := make([]string, 0, len(entry.WordPos))

	for key := range entry.WordPos {
		ret = append(ret, key)
	}

	sort.StringSlice(ret).Sort()

	return ret, nil
}

/*
Count returns the number of found nodes for a given word in a given attribute.
*/
func (im *IndexManager) Count(attr, word string) (int, error) {
	var s string

	if CaseSensitiveWordIndex {
		s = word
	} else {
		s = strings.ToLower(word)
	}

	entry, err := im.htree.Get([]byte(PrefixAttrWord + attr + s))

	if err != nil {
		return 0, &GraphError{ErrIndexError, err.Error()}
	} else if entry == nil {
		return 0, nil
	}

	return len(entry.(*indexEntry).WordPos), nil
}

/*
updateIndex updates the index for a specific object. Depending on the
new and old arguments being set a given object is either indexed/added
(only new is set), deindexted/removed (only old is set) or reindexted/updated
(new and old are set).
*/
func (im *IndexManager) updateIndex(key string, newObj map[string]string,
	oldObj map[string]string) error {

	attrMap := make(map[string][]byte)

	if newObj != nil && oldObj == nil {

		// Insert case

		for attr := range newObj {
			attrMap[attr] = nil
		}

	} else if newObj == nil && oldObj != nil {

		// Remove case

		for attr := range oldObj {
			attrMap[attr] = nil
		}

	} else {

		// Update case

		for attr := range newObj {
			attrMap[attr] = nil
		}
		for attr := range oldObj {
			attrMap[attr] = nil
		}
	}

	emptyws := newWordSet(1)

	for attr := range attrMap {
		var newwords, toadd, oldwords, toremove *wordSet

		newval, newok := newObj[attr]
		oldval, oldok := oldObj[attr]

		// Calculate which words to add or remove

		newwords = emptyws
		oldwords = emptyws

		if newok {
			newwords = extractWords(newval)
		}

		// At this point we have only words to add

		toadd = newwords
		toremove = emptyws

		if oldok {
			oldwords = extractWords(oldval)

			if !oldwords.Empty() && !newwords.Empty() {

				// Here a diff is necessary

				toadd = copyWordSet(newwords)
				toadd.RemoveAll(oldwords)

				toremove = oldwords
				toremove.RemoveAll(newwords)

			} else {

				// Either no new words or no old words

				toremove = oldwords
			}
		}

		// Add and remove index entries

		for w, p := range toremove.set {
			if err := im.removeIndexEntry(key, attr, w, p); err != nil {
				return &GraphError{ErrIndexError, err.Error()}
			}
		}

		for w, p := range toadd.set {
			if err := im.addIndexEntry(key, attr, w, p); err != nil {
				return &GraphError{ErrIndexError, err.Error()}
			}
		}

		// Update hash lookup

		if newok && oldok {

			// Update hash entry

			if err := im.removeIndexHashEntry(key, attr, oldval); err != nil {
				return &GraphError{ErrIndexError, err.Error()}
			} else if err := im.addIndexHashEntry(key, attr, newval); err != nil {
				return &GraphError{ErrIndexError, err.Error()}
			}

		} else if newok && !oldok {

			// Insert hash entry

			if err := im.addIndexHashEntry(key, attr, newval); err != nil {
				return &GraphError{ErrIndexError, err.Error()}
			}

		} else if oldok {

			// Delete old hash entry

			if err := im.removeIndexHashEntry(key, attr, oldval); err != nil {
				return &GraphError{ErrIndexError, err.Error()}
			}
		}
	}

	return nil
}

/*
addIndexHashEntry add a hash entry from the index. A hash entry stores a whole
value as MD5 sum.
*/
func (im *IndexManager) addIndexHashEntry(key string, attr string, value string) error {
	var entry *indexEntry
	var sum [16]byte

	if CaseSensitiveWordIndex {
		sum = md5.Sum([]byte(value))
	} else {
		sum = md5.Sum([]byte(strings.ToLower(value)))
	}

	indexkey := []byte(PrefixAttrHash + attr + string(sum[:16]))

	// Retrieve index entry

	obj, err := im.htree.Get(indexkey)
	if err != nil {
		return err
	}

	if obj == nil {
		entry = &indexEntry{make(map[string]string)}
	} else {
		entry = obj.(*indexEntry)
	}

	entry.WordPos[key] = ""

	_, err = im.htree.Put(indexkey, entry)

	return err
}

/*
removeIndexHashEntry removes a hash entry from the index. A hash entry stores a whole
value as MD5 sum.
*/
func (im *IndexManager) removeIndexHashEntry(key string, attr string, value string) error {
	var entry *indexEntry
	var sum [16]byte

	if CaseSensitiveWordIndex {
		sum = md5.Sum([]byte(value))
	} else {
		sum = md5.Sum([]byte(strings.ToLower(value)))
	}

	indexkey := []byte(PrefixAttrHash + attr + string(sum[:16]))

	// Retrieve index entry

	obj, err := im.htree.Get(indexkey)

	if err != nil {
		return err
	}

	if obj == nil {
		return nil
	}

	entry = obj.(*indexEntry)

	delete(entry.WordPos, key)

	if len(entry.WordPos) == 0 {
		im.htree.Remove(indexkey)
	} else {
		im.htree.Put(indexkey, entry)
	}

	return err
}

/*
removeIndexEntry removes an entry from the index.
*/
func (im *IndexManager) removeIndexEntry(key string, attr string, word string, pos []uint64) error {
	var entry *indexEntry

	indexkey := []byte(PrefixAttrWord + attr + word)

	// Retrieve index entry

	obj, err := im.htree.Get(indexkey)
	if err != nil {
		return err
	}

	if obj == nil {
		return nil
	}

	entry = obj.(*indexEntry)

	// Remove given pos from existing pos information

	if keyentry, ok := entry.WordPos[key]; ok {

		keyentrylist := bitutil.UnpackList(keyentry)
		res := make([]uint64, 0, len(keyentrylist))

		remLookup := make(map[uint64]bool)
		for _, item := range pos {
			remLookup[item] = true
		}

		for _, item := range keyentrylist {
			if _, ok := remLookup[item]; !ok {
				res = append(res, item)
			}
		}

		if len(res) == 0 {
			delete(entry.WordPos, key)
		} else {
			entry.WordPos[key] = bitutil.PackList(res, res[len(res)-1])
		}
	}

	if len(entry.WordPos) == 0 {
		_, err = im.htree.Remove(indexkey)
	} else {
		_, err = im.htree.Put(indexkey, entry)
	}

	return err
}

/*
addIndexEntry adds an entry to the index.
*/
func (im *IndexManager) addIndexEntry(key string, attr string, word string, pos []uint64) error {
	var entry *indexEntry

	indexkey := []byte(PrefixAttrWord + attr + word)

	// Retrieve or create index entry

	obj, err := im.htree.Get(indexkey)
	if err != nil {
		return err
	}

	if obj == nil {
		entry = &indexEntry{make(map[string]string)}
	} else {
		entry = obj.(*indexEntry)
	}

	// Create position string

	if len(pos) == 0 {
		panic("Trying to add index entry without position information")
	}

	// Mix in given pos with existing pos information

	if keyentry, ok := entry.WordPos[key]; ok {

		pos = append(bitutil.UnpackList(keyentry), pos...)
		sortutil.UInt64s(pos)
		pos = removeDuplicates(pos)
	}

	// Rely on the fact that position arrays are ordered in ascending order

	maxpos := pos[len(pos)-1]

	// Fill the entry and store it

	entry.WordPos[key] = bitutil.PackList(pos, maxpos)

	_, err = im.htree.Put(indexkey, entry)

	return err
}

/*
Remove all duplicates from a given sorted list.
*/
func removeDuplicates(list []uint64) []uint64 {

	if len(list) == 0 {
		return list
	}

	res := make([]uint64, 1, len(list))
	res[0] = list[0]

	last := list[0]

	for _, item := range list[1:] {
		if item != last {
			res = append(res, item)
			last = item
		}
	}

	return res
}

/*
String returns a string representation of this index manager.
*/
func (im *IndexManager) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("IndexManager: %v\n", im.htree.Location()))

	it := hash.NewHTreeIterator(im.htree)

	for it.HasNext() {
		key, value := it.Next()

		posmap := make(map[string][]uint64)
		for k, v := range value.(*indexEntry).WordPos {
			posmap[k] = bitutil.UnpackList(v)
		}

		buf.WriteString(fmt.Sprintf("    %v%q %v\n", key[0], string(key[1:]), posmap))
	}

	return buf.String()
}

/*
extractWords extracts all words from a given string and return a wordSet which contains
all words and their positions.
*/
func extractWords(s string) *wordSet {

	var text string

	if CaseSensitiveWordIndex {
		text = s
	} else {
		text = strings.ToLower(s)
	}

	initArrCap := int(math.Ceil(float64(len(text)) * 0.01))
	if initArrCap < 4 {
		initArrCap = 4
	}

	ws := newWordSet(initArrCap)

	var pos uint64
	wstart := -1

	for i, rune := range text {

		if !stringutil.IsAlphaNumeric(string(rune)) && (unicode.IsSpace(rune) || unicode.IsControl(rune) || unicode.IsPunct(rune)) {

			if wstart >= 0 {
				ws.Add(text[wstart:i], pos+1)
				pos++
				wstart = -1
			}

		} else if wstart == -1 {
			wstart = i
		}
	}

	if wstart >= 0 {
		ws.Add(text[wstart:], pos+1)
	}

	return ws
}

/*
Internal data structure for sets of words and their positions.
*/
type wordSet struct {
	set        map[string][]uint64 // Map which holds the data
	initArrCap int                 // Initial capacity for the position array
}

/*
newWordSet creates a new word set.
*/
func newWordSet(initArrCap int) *wordSet {
	return &wordSet{make(map[string][]uint64), initArrCap}
}

/*
copyWordSet creates a new word set from a present one.
*/
func copyWordSet(ws *wordSet) *wordSet {
	ret := &wordSet{make(map[string][]uint64), ws.initArrCap}
	ret.AddAll(ws)

	return ret
}

/*
Add adds a word to the word set. Returns true if the word was added and false
if an existing entry was updated.
*/
func (ws *wordSet) Add(s string, pos uint64) bool {
	v, ok := ws.set[s]

	if !ok {
		ws.set[s] = make([]uint64, 1, ws.initArrCap)
		ws.set[s][0] = pos

	} else {

		// Make sure the largest entry is always last

		l := len(ws.set[s])

		if ws.set[s][l-1] < pos {
			ws.set[s] = append(v, pos)
		} else {

			// Make sure there is no double entry

			for _, ex := range v {
				if ex == pos {
					return !ok
				}
			}

			ws.set[s] = append(v, pos)
			sortutil.UInt64s(ws.set[s])
		}
	}

	return !ok
}

/*
AddAll adds all words from another word set to this word set.
*/
func (ws *wordSet) AddAll(ws2 *wordSet) {
	for s, val := range ws2.set {
		for _, v := range val {
			ws.Add(s, v)
		}
	}
}

/*
Empty checks if this word set is empty.
*/
func (ws *wordSet) Empty() bool {
	return len(ws.set) == 0
}

/*
Has checks if this word set has a certain word.
*/
func (ws *wordSet) Has(s string) bool {
	_, ok := ws.set[s]
	return ok
}

/*
Pos returns the positions of a certain word.
*/
func (ws *wordSet) Pos(s string) []uint64 {
	if pos, ok := ws.set[s]; ok {
		return pos
	}
	return nil
}

/*
Remove removes a word from the word set.
*/
func (ws *wordSet) Remove(s string, pos uint64) {
	if posArr, ok := ws.set[s]; ok {

		// Look for the position

		for i, p := range posArr {
			if p == pos {
				posArr := append(posArr[:i], posArr[i+1:]...)
				ws.set[s] = posArr
				break
			}
		}

		// Remove the word if no more positions are left

		if len(ws.set[s]) == 0 {
			delete(ws.set, s)
		}
	}
}

/*
RemoveAll removes all words from another word set from this word set.
*/
func (ws *wordSet) RemoveAll(ws2 *wordSet) {
	for s, posArr2 := range ws2.set {

		if posArr, ok := ws.set[s]; ok {

			j := 0
			for i := 0; i < len(posArr2); i++ {
				for ; j < len(posArr); j++ {

					if posArr[j] == posArr2[i] {

						// If a matching entry was found remove it

						posArr = append(posArr[:j], posArr[j+1:]...)
						ws.set[s] = posArr
						break

					} else if posArr[j] > posArr2[i] {

						// Skip over if a position is not in the current posArr

						break
					}
				}
			}
		}

		// Remove the word if no more positions are left

		if len(ws.set[s]) == 0 {
			delete(ws.set, s)
		}
	}
}

/*
String returns a string representation of this word set.
*/
func (ws *wordSet) String() string {
	var buf bytes.Buffer
	c := make([]string, 0, len(ws.set))

	for s := range ws.set {
		c = append(c, s)
	}

	sort.StringSlice(c).Sort()

	buf.WriteString("WordSet:\n")

	for _, k := range c {
		buf.WriteString(fmt.Sprintf("    %v %v\n", k, ws.set[k]))
	}

	return buf.String()
}
