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
Name manager managing names of kinds, roles and attributes. Each stored name
gets either a 16 or 32 bit (little endian) number assigned. The manager provides functions to lookup
these names or their numbers.
*/
package util

import "encoding/binary"

/*
Prefix for entries storing codes
*/
const PREFIX_CODE = string(0x0)

/*
Prefix for entries storing names
*/
const PREFIX_NAME = string(0x1)

/*
Prefix for counter entries
*/
const PREFIX_COUNTER = string(0x0)

/*
Prefix for 16 bit kind related entries
*/
const PREFIX_16_BIT = string(0x1)

/*
Prefix for attribute related entries
*/
const PREFIX_32_BIT = string(0x2)

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
	return gs.encode(PREFIX_32_BIT, val, create)
}

/*
Decode32 decodes a given 32 bit string to a value.
*/
func (gs *NamesManager) Decode32(val string) string {
	return gs.decode(PREFIX_32_BIT, val)
}

/*
Encode16 encodes a given value as a 16 bit string. If the create flag
is set to false then a new entry will not be created if it does not exist.
*/
func (gs *NamesManager) Encode16(val string, create bool) string {
	return gs.encode(PREFIX_16_BIT, val, create)
}

/*
Decode16 decodes a given 16 bit string to a value.
*/
func (gs *NamesManager) Decode16(val string) string {
	return gs.decode(PREFIX_16_BIT, val)
}

/*
encode encodes a name to a code.
*/
func (gs *NamesManager) encode(prefix string, name string, create bool) string {
	codekey := string(PREFIX_CODE) + prefix + name

	code, ok := gs.nameDB[codekey]

	// If the code doesn't exist yet create it

	if !ok && create {
		if prefix == PREFIX_16_BIT {
			code = gs.newCode16()

		} else {
			code = gs.newCode32()
		}

		gs.nameDB[codekey] = code
		namekey := string(PREFIX_NAME) + prefix + code
		gs.nameDB[namekey] = name
	}

	return code
}

/*
decode decodes a name from a code.
*/
func (gs *NamesManager) decode(prefix string, code string) string {
	namekey := string(PREFIX_NAME) + prefix + code

	return gs.nameDB[namekey]
}

/*
newCode32 generates a new 32 bit number for the names map.
*/
func (gs *NamesManager) newCode32() (res string) {
	var resnum uint32

	// Calculate count entry

	countAttr := string(PREFIX_COUNTER) + PREFIX_32_BIT

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

	countAttr := string(PREFIX_COUNTER) + PREFIX_16_BIT

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
