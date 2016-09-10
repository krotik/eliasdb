/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package view

/*
TypeFreePage is a free page waiting to be (re)allocated
*/
const TypeFreePage = 0

/*
TypeDataPage is a page which is in use and contains data
*/
const TypeDataPage = 1

/*
TypeTranslationPage is a page which translates between physical and logical row ids
*/
const TypeTranslationPage = 2

/*
TypeFreeLogicalSlotPage is a page which holds free logical slot ids
(used to give stable ids to objects which can grow in size)
*/
const TypeFreeLogicalSlotPage = 3

/*
TypeFreePhysicalSlotPage is a page which holds free physical slot ids
*/
const TypeFreePhysicalSlotPage = 4
