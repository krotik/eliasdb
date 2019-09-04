/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package pageview

import (
	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging/view"
	"devt.de/krotik/eliasdb/storage/util"
)

/*
SlotInfoPage data structure
*/
type SlotInfoPage struct {
	*view.PageView
}

/*
NewSlotInfoPage creates a new SlotInfoPage object which can manage slotinfos.
*/
func NewSlotInfoPage(record *file.Record) *SlotInfoPage {
	pv := view.GetPageView(record)
	return &SlotInfoPage{pv}
}

/*
SlotInfoRecord gets record id of a stored slotinfo.
*/
func (lm *SlotInfoPage) SlotInfoRecord(offset uint16) uint64 {
	return util.LocationRecord(lm.Record.ReadUInt64(int(offset)))
}

/*
SlotInfoOffset gets the record offset of a stored slotinfo.
*/
func (lm *SlotInfoPage) SlotInfoOffset(offset uint16) uint16 {
	return util.LocationOffset(lm.Record.ReadUInt64(int(offset)))
}

/*
SetSlotInfo stores a slotinfo on the pageview's record.
*/
func (lm *SlotInfoPage) SetSlotInfo(slotinfoOffset uint16, recordID uint64, offset uint16) {
	lm.Record.WriteUInt64(int(slotinfoOffset), util.PackLocation(recordID, offset))
}
