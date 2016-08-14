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
This file contains all known types of pageviews.
*/
package view

/*
A free page waiting to be (re)allocated
*/
const TYPE_FREE_PAGE = 0

/*
A page which is in use and contains data
*/
const TYPE_DATA_PAGE = 1

/*
Translation pages translate between physical and logical row ids
*/
const TYPE_TRANSLATION_PAGE = 2

/*
Free logical slot pages hold free logical slot ids
(used to give stable ids to objects which can grow in size)
*/
const TYPE_FREE_LOGICAL_SLOT_PAGE = 3

/*
Free physical slot pages hold free physical slot ids
*/
const TYPE_FREE_PHYSICAL_SLOT_PAGE = 4
