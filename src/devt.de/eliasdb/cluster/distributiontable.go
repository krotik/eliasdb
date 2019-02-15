/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"math"
)

/*
defaultDistributionRange is the default range of possible addresses for any cluster.
Depending on the cluster size each member is in charge of a certain part of this range.
*/
var defaultDistributionRange = uint64(math.MaxUint64)

/*
DistributionTable is used to locate data in a cluster. The table contains
all cluster members and can identify replication members for given data locations.
*/
type DistributionTable struct {
	members     []string            // All known cluster members
	memberRange uint64              // Range for a single member
	distrange   []uint64            // Distribution range among members
	mranges     map[string]uint64   // Map member ranges
	replicas    map[string][]string // Map of replicas (which members a replicas of a member)
	replicates  map[string][]string // Map of replicates (what is replicated on a member)
	repFac      int                 // Replication factor of the cluster
	space       uint64              // Address space which is distributed in the cluster
}

/*
NewDistributionTable creates a new distribution table.
*/
func NewDistributionTable(members []string, repFac int) (*DistributionTable, error) {
	return createDistributionTable(members, repFac, defaultDistributionRange)
}

/*
createDistributionTable creates a new distribution table.
*/
func createDistributionTable(members []string, repFac int, space uint64) (*DistributionTable, error) {
	var distrange []uint64

	replicas := make(map[string][]string)
	replicates := make(map[string][]string)
	mranges := make(map[string]uint64)

	// Check for bogus values

	if repFac < 1 {
		return nil, errors.New("Replication factor must be > 0")
	} else if repFac > len(members) {
		return nil, fmt.Errorf("Not enough members (%v) for given replication factor: %v",
			len(members), repFac)
	}

	// Do the range calculations

	memberRange := uint64(space / uint64(len(members)))
	for i := 0; i < len(members); i++ {
		mrange := uint64(i) * memberRange

		mranges[members[i]] = mrange
		distrange = append(distrange, mrange)

		var replicasList []string

		for j := 1; j < repFac; j++ {
			replicasList = append(replicasList, members[(i+j)%len(members)])
		}

		replicas[members[i]] = replicasList

		replicates[members[i]] = make([]string, 0, repFac)
	}

	for m, r := range replicas {
		for _, rm := range r {
			replicates[rm] = append(replicates[rm], m)
		}
	}

	return &DistributionTable{members, memberRange, distrange, mranges, replicas,
		replicates, repFac, space}, nil
}

/*
Members returns all known cluster members.
*/
func (dd *DistributionTable) Members() []string {
	return dd.members
}

/*
Replicas returns all replicas for a given member.
*/
func (dd *DistributionTable) Replicas(name string) []string {
	return dd.replicas[name]
}

/*
MemberRange returns the location range of a given member.
*/
func (dd *DistributionTable) MemberRange(name string) (uint64, uint64) {
	mrange := dd.mranges[name]
	if name == dd.members[len(dd.members)-1] {
		return mrange, dd.space
	}
	return mrange, mrange + dd.memberRange - 1
}

/*
ReplicationRange return the location range which is replicated by a given member.
*/
func (dd *DistributionTable) ReplicationRange(name string) (uint64, uint64) {
	var start, end uint64

	start = defaultDistributionRange

	for _, r := range dd.replicates[name] {

		rstart, rend := dd.MemberRange(r)

		if rstart < start {
			start = rstart
		}

		if rend > end {
			end = rend
		}
	}

	return start, end
}

/*
LocationHome return the member which is in charge of a given location and all its replicas.
*/
func (dd *DistributionTable) LocationHome(loc uint64) (string, []string) {
	var member string

	for i, r := range dd.distrange {
		if loc < r {
			member = dd.members[i-1]
			return member, dd.replicas[member]
		}
	}

	member = dd.members[len(dd.members)-1]

	return member, dd.replicas[member]
}

/*
OtherReplicationMembers returns all members of a replication group (identified
by a given locqtion) minus a given member.
*/
func (dd *DistributionTable) OtherReplicationMembers(loc uint64, name string) []string {
	var ret []string

	primary, replicas := dd.LocationHome(loc)

	if name == primary {
		ret = replicas
	} else {
		ret = append(ret, primary)
		for _, rep := range replicas {
			if rep != name {
				ret = append(ret, rep)
			}
		}
	}

	return ret
}

/*
String returns a string representation of this distribution table.
*/
func (dd *DistributionTable) String() string {
	var ret bytes.Buffer

	ret.WriteString("Location ranges:\n")

	for _, member := range dd.members {
		f, t := dd.MemberRange(member)
		ret.WriteString(fmt.Sprintf("%v: %v -> %v\n", member, f, t))
	}

	ret.WriteString(fmt.Sprintf("Replicas (factor=%v) :\n", dd.repFac))

	for _, member := range dd.members {
		ret.WriteString(fmt.Sprintf("%v: %v\n", member, dd.replicas[member]))
	}

	return ret.String()
}
