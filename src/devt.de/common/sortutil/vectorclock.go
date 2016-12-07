package sortutil

import (
	"bytes"
	"fmt"
	"sort"
)

/*
VectorClock implements a vector clock object. The clock can record actions
of actors. Each action produces a new version which can be queried.
*/
type VectorClock struct {
	versionVector map[string]uint64 // Data for the cache
}

/*
NewVectorClock creates a new vector clock datastructure.
*/
func NewVectorClock() *VectorClock {
	return &VectorClock{make(map[string]uint64)}
}

/*
CloneVectorClock clones an existing vector clock.
*/
func CloneVectorClock(vc *VectorClock) *VectorClock {
	newVC := NewVectorClock()
	for actor, version := range vc.versionVector {
		newVC.versionVector[actor] = version
	}
	return newVC
}

/*
NewDescendant creates a vector clock which is a descendant of all given vector clocks.
*/
func NewDescendant(otherVCs ...*VectorClock) *VectorClock {
	newVC := NewVectorClock()

	for _, otherVC := range otherVCs {
		for actor, version := range otherVC.versionVector {
			if newVC.Version(actor) < version {
				newVC.versionVector[actor] = version
			}
		}
	}

	return newVC
}

/*
Act records an action of an actor.
*/
func (vc *VectorClock) Act(actor string) {
	if _, ok := vc.versionVector[actor]; ok {
		vc.versionVector[actor]++
	} else {
		vc.versionVector[actor] = 1
	}
}

/*
Version returns the current version for a given actor.
*/
func (vc *VectorClock) Version(actor string) uint64 {
	if v, ok := vc.versionVector[actor]; ok {
		return v
	}
	return 0
}

/*
IsDescendent determines if another vector clock is a descendent of this vector clock.
*/
func (vc *VectorClock) IsDescendent(otherVC *VectorClock) bool {

	// In order for vc to be considered a descendant of otherVC, each marker
	// in otherVC must have a corresponding marker in vc that has a revision
	// number greater than or equal to the marker in otherVC.

	for actor, version := range otherVC.versionVector {
		if vc.Version(actor) < version {
			return false
		}
	}

	return true
}

/*
IsConflicting determines if another vector clock is conflicting with this vector clock.
*/
func (vc *VectorClock) IsConflicting(otherVC *VectorClock) bool {
	return !(vc.IsDescendent(otherVC) || otherVC.IsDescendent(vc))
}

/*
String returns a string representation of this vector clock.
*/
func (vc *VectorClock) String() string {

	var actors []string
	for actor := range vc.versionVector {
		actors = append(actors, actor)
	}

	sort.Strings(actors)

	buf := &bytes.Buffer{}

	for _, actor := range actors {
		version := vc.versionVector[actor]
		buf.WriteString(fmt.Sprint(actor, ":", version, "\n"))
	}

	return buf.String()
}
