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

import "encoding/binary"

/*
PrefixCode is the prefix for entries storing codes
*/
const PrefixCode = "\x00"

/*
PrefixName is the prefix for entries storing names
*/
const PrefixName = "\x01"

/*
PrefixCounter is the prefix for counter entries
*/
const PrefixCounter = "\x00"

/*
Prefix16Bit is the prefix for 16 bit kind related entries
*/
const Prefix16Bit = "\x01"

/*
Prefix32Bit is the prefix for attribute related entries
*/
const Prefix32Bit = "\x02"

/*
NamesManager data structure
*/
type NamesManager struct {
	nameDB map[string]string // Database storing names
}

/*
NewNamesManager creates a new names manager instance.
*/
func NewNamesManager(nameDB map[string]string) *NamesManager {
	return &NamesManager{nameDB}
}

/*
Encode32 encodes a given value as a 32 bit string. If the create flag
is set to false then a new entry will not be created if it does not exist.
*/
func (gs *NamesManager) Encode32(val string, create bool) string {
	return gs.encode(Prefix32Bit, val, create)
}

/*
Decode32 decodes a given 32 bit string to a value.
*/
func (gs *NamesManager) Decode32(val string) string {
	return gs.decode(Prefix32Bit, val)
}

/*
Encode16 encodes a given value as a 16 bit string. If the create flag
is set to false then a new entry will not be created if it does not exist.
*/
func (gs *NamesManager) Encode16(val string, create bool) string {
	return gs.encode(Prefix16Bit, val, create)
}

/*
Decode16 decodes a given 16 bit string to a value.
*/
func (gs *NamesManager) Decode16(val string) string {
	return gs.decode(Prefix16Bit, val)
}

/*
encode encodes a name to a code.
*/
func (gs *NamesManager) encode(prefix string, name string, create bool) string {
	codekey := string(PrefixCode) + prefix + name

	code, ok := gs.nameDB[codekey]

	// If the code doesn't exist yet create it

	if !ok && create {
		if prefix == Prefix16Bit {
			code = gs.newCode16()

		} else {
			code = gs.newCode32()
		}

		gs.nameDB[codekey] = code
		namekey := string(PrefixName) + prefix + code
		gs.nameDB[namekey] = name
	}

	return code
}

/*
decode decodes a name from a code.
*/
func (gs *NamesManager) decode(prefix string, code string) string {
	namekey := string(PrefixName) + prefix + code

	return gs.nameDB[namekey]
}

/*
newCode32 generates a new 32 bit number for the names map.
*/
func (gs *NamesManager) newCode32() (res string) {
	var resnum uint32

	// Calculate count entry

	countAttr := string(PrefixCounter) + Prefix32Bit

	// Calculate new code

	val, ok := gs.nameDB[countAttr]
	if !ok {
		resnum = 1
	} else {
		resnum = binary.LittleEndian.Uint32([]byte(val))
		resnum++
	}

	// Convert to a string

	resStr := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(resStr, resnum)
	res = string(resStr)

	// Write back

	gs.nameDB[countAttr] = res

	return res
}

/*
newCode16 generates a new 16 bit number for the names map.
*/
func (gs *NamesManager) newCode16() (res string) {
	var resnum uint16

	// Calculate count entry

	countAttr := string(PrefixCounter) + Prefix16Bit

	// Calculate new code

	val, ok := gs.nameDB[countAttr]
	if !ok {
		resnum = 1
	} else {
		resnum = binary.LittleEndian.Uint16([]byte(val))
		resnum++
	}

	// Convert to a string

	resStr := make([]byte, 2, 2)
	binary.LittleEndian.PutUint16(resStr, resnum)
	res = string(resStr)

	// Write back

	gs.nameDB[countAttr] = res

	return res
}
