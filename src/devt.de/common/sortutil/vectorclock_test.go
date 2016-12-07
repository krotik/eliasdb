/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package sortutil

import "testing"

type dinnerDay struct {
	day string
	vc  *VectorClock
}

const (
	actorAlice = "Alice"
	actorBen   = "Ben"
	actorCathy = "Cathy"
	actorDave  = "Dave"
)

/*
The dinner agreement example was taken from:
http://basho.com/posts/technical/why-vector-clocks-are-easy/
*/

func TestDinnerAgreement(t *testing.T) {

	// Test how Alice, Ben Cathy and Dave are meeting for dinner at Dave's place

	// Start by Alice suggesting to meet on Wednesday

	dd := &dinnerDay{"Wednesday", NewVectorClock()}
	dd.vc.Act(actorAlice)

	dd2 := &dinnerDay{dd.day, CloneVectorClock(dd.vc)}

	// Ben suggests now Tuesday

	dd.day = "Tuesday"
	dd.vc.Act(actorBen)

	// Dave confirms the day

	dd.vc.Act(actorDave)

	// Check descendancy

	if !dd.vc.IsDescendent(dd2.vc) {
		t.Error("dd should be a descendent of dd2")
		return
	} else if dd2.vc.IsDescendent(dd.vc) {
		t.Error("dd2 should not be a descendent of dd")
		return
	}

	// Cathy has an old version and suggests Thursday

	dd2.day = "Thursday"
	dd2.vc.Act(actorCathy)

	// Detect conflict

	if !dd.vc.IsConflicting(dd2.vc) {
		t.Error("Vector clocks should be conflicting")
		return
	}

	// Dave makes a decision and chooses Thursday

	dd3 := &dinnerDay{dd2.day, NewDescendant(dd.vc, dd2.vc)}
	dd3.vc.Act(actorDave)

	// Check descendancy

	if !dd3.vc.IsDescendent(dd.vc) || dd3.vc.IsConflicting(dd.vc) {
		t.Error("dd3 should be a descendent of dd")
		return
	} else if !dd3.vc.IsDescendent(dd2.vc) || dd3.vc.IsConflicting(dd2.vc) {
		t.Error("dd3 should be a descendent of dd2")
		return
	}

	if out := dd3.vc.String(); out != `
Alice:1
Ben:1
Cathy:1
Dave:2
`[1:] {
		t.Error("Unexpected output:", out)
		return
	}
}
