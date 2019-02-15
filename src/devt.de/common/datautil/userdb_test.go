/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package datautil

import (
	"fmt"
	"path"
	"testing"
)

func TestUserDB(t *testing.T) {

	// Create user DB instance and store a credential

	ud, err := NewUserDB(path.Join(testdbdir, "testuserdb"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	err = ud.AddUserEntry("fred", "s3cret", map[string]interface{}{
		"field1": "foo",
		"field2": 2,
	})
	if err != nil {
		t.Error(err)
		return
	}

	// Create a second user DB instance

	ud2, err := NewUserDB(path.Join(testdbdir, "testuserdb"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the data was loaded

	if res := fmt.Sprint(ud2.AllUsers()); res != "[fred]" {
		t.Error("Unexpected result:", res)
		return
	}

	// Get the user data

	data, ok := ud2.UserData("fred")

	if !ok || data["field1"] != "foo" || data["field2"] != 2 {
		t.Error("Unexpected result:", ok, data)
		return
	}

	// Check user password

	if ok := ud2.CheckUserPassword("fred", "s3cret"); !ok || err != nil {
		t.Error("Unexpected result:", ok, err)
		return
	}

	if ok := ud2.CheckUserPassword("fred", "s4cret"); ok || err != nil {
		t.Error("Unexpected result:", ok, err)
		return
	}

	// Change data and password

	err = ud2.UpdateUserPassword("fred", "secret55")
	if err != nil {
		t.Error(err)
		return
	}

	err = ud2.UpdateUserData("fred", map[string]interface{}{
		"field5": "bar",
		"field2": 2,
	})
	if err != nil {
		t.Error(err)
		return
	}

	// ... and another instance

	ud3, err := NewUserDB(path.Join(testdbdir, "testuserdb"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	// Check that all updated information are correct

	data, ok = ud3.UserData("fred")

	if !ok || data["field5"] != "bar" || data["field2"] != 2 {
		t.Error("Unexpected result:", ok, data)
		return
	}

	// Check user password

	if ok := ud3.CheckUserPassword("fred", "s3cret"); ok || err != nil {
		t.Error("Unexpected result:", ok, err)
		return
	}

	if ok := ud3.CheckUserPassword("fred", "secret55"); !ok || err != nil {
		t.Error("Unexpected result:", ok, err)
		return
	}

	// Remove now the entry

	ud3.RemoveUserEntry("fred")

	ud4, err := NewUserDB(path.Join(testdbdir, "testuserdb"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the data was removed

	if res := fmt.Sprint(ud4.AllUsers()); res != "[]" {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestUserDBPasswordHistory(t *testing.T) {
	oldMaxPassHistory := MaxPassHistory
	MaxPassHistory = 3
	defer func() {
		MaxPassHistory = oldMaxPassHistory
	}()

	// Create user DB instance and store a credential

	ud, err := NewUserDB(path.Join(testdbdir, "testuserdbhistory"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	if err = ud.AddUserEntry("fred", "s3cret1", nil); err != nil {
		t.Error(err)
		return
	}

	if len(ud.Data["fred"].PasshashHistory) != 0 {
		t.Error("Unexpected result:", ud.Data["fred"].PasshashHistory)
		return
	}

	if err = ud.UpdateUserPassword("fred", "s3cret1"); err.Error() != "Cannot reuse current password" {
		t.Error(err)
		return
	}

	if err = ud.UpdateUserPassword("fred", "s3cret2"); err != nil {
		t.Error(err)
		return
	}

	if len(ud.Data["fred"].PasshashHistory) != 1 {
		t.Error("Unexpected result:", ud.Data["fred"].PasshashHistory)
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret1"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret2"); ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	ud.UpdateUserPassword("fred", "s3cret3")

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret1"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret2"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if len(ud.Data["fred"].PasshashHistory) != 2 {
		t.Error("Unexpected result:", ud.Data["fred"].PasshashHistory)
		return
	}

	ud.UpdateUserPassword("fred", "s3cret4")

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret1"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret2"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret3"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if len(ud.Data["fred"].PasshashHistory) != 3 {
		t.Error("Unexpected result:", ud.Data["fred"].PasshashHistory)
		return
	}

	ud.UpdateUserPassword("fred", "s3cret5")

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret2"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret3"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret4"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if len(ud.Data["fred"].PasshashHistory) != 3 {
		t.Error("Unexpected result:", ud.Data["fred"].PasshashHistory)
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret1"); ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	ud.UpdateUserPassword("fred", "s3cret6")

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret3"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret4"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret5"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if len(ud.Data["fred"].PasshashHistory) != 3 {
		t.Error("Unexpected result:", ud.Data["fred"].PasshashHistory)
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret2"); ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPasswordHistory("fred", "s3cret6"); ok || err != nil {
		t.Error("Unexpected result")
		return
	}

	if ok := ud.CheckUserPassword("fred", "s3cret6"); !ok || err != nil {
		t.Error("Unexpected result")
		return
	}
}

func TestUserDBErrorCases(t *testing.T) {

	ud, err := NewUserDB(path.Join(testdbdir, invalidFileName), "test123")

	if err == nil || ud != nil {
		t.Error("Unexpected result:", err, ud)
		return
	}

	ud, err = NewUserDB(path.Join(testdbdir, "errtest"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	err = ud.AddUserEntry("foo", "bar", nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = ud.AddUserEntry("foo", "bar", nil)
	if err == nil || err.Error() != "User foo already exists" {
		t.Error(err)
		return
	}

	err = ud.UpdateUserData("fred", nil)
	if err == nil || err.Error() != "Unknown user fred" {
		t.Error(err)
		return
	}

	err = ud.UpdateUserPassword("fred", "")
	if err == nil || err.Error() != "Unknown user fred" {
		t.Error(err)
		return
	}

	err = ud.RemoveUserEntry("fred")
	if err == nil || err.Error() != "Unknown user fred" {
		t.Error(err)
		return
	}

}

func TestEnforcedUserDB(t *testing.T) {

	// Create user DB instance and store a credential

	eud, err := NewEnforcedUserDB(path.Join(testdbdir, "testenforceuserdb"), "test123")
	if err != nil {
		t.Error(err)
		return
	}

	eud.SetPasswordCheckParam("NotContainSequence", false)

	if err := eud.AddUserEntry("fritz", "#Secr3taaa", nil); err != nil {
		t.Error(err)
		return
	}

	if eud.UserExists("foo") {
		t.Error("User foo should not exist")
		return
	}

	if !eud.UserExists("fritz") {
		t.Error("User fritz should exist")
		return
	}

	eud.SetPasswordCheckParam("NotContainSequence", true)

	if res := len(eud.PasswordCheckParams()); res != 8 {
		t.Error("Unexpected result:", res)
		return
	}

	if err := eud.UpdateUserPassword("fritz", "#Secr3tbbb"); err.Error() != "Password must not contain a same character sequence" {
		t.Error(err)
		return
	}

	if err := eud.UpdateUserPassword("fritz", "#Secr3tabc"); err != nil {
		t.Error(err)
		return
	}

	if err := eud.UpdateUserPassword("fritz", "#Secr3taaa"); err.Error() != "Password was used before within the last 10 changes; Password must not contain a same character sequence" {
		t.Error(err)
		return
	}

	if err := eud.AddUserEntry("hans", "aaa", nil); err.Error() != "Password matches a common dictionary password; Password must be at least 8 characters long; Password must contain an upper case character; Password must contain a number; Password must contain a special character; Password must not contain a same character sequence" {
		t.Error(err)
		return
	}

	// Test multiple errors

	if err := eud.UpdateUserPassword("fritz", "aaa"); err == nil || err.Error() != "Password matches a common dictionary password; Password must be at least 8 characters long; Password must contain an upper case character; Password must contain a number; Password must contain a special character; Password must not contain a same character sequence" {
		t.Error(err)
		return
	}
	if err := eud.IsAcceptablePassword("fritz", "#Secr3tabc"); err == nil || err.Error() != "Cannot reuse current password" {
		t.Error(err)
		return
	}
	if err := eud.IsAcceptablePassword("fritz", "AA1"); err == nil || err.Error() != "Password is too similar to the common dictionary password aa1234 (50% match); Password must be at least 8 characters long; Password must contain a lower case character; Password must contain a special character" {
		t.Error(err)
		return
	}
	if err := eud.IsAcceptablePassword("fritz", "xxx"); err == nil || err.Error() != "Password must be at least 8 characters long; Password must contain an upper case character; Password must contain a number; Password must contain a special character; Password must not contain a same character sequence" {
		t.Error(err)
		return
	}

	if err := eud.IsAcceptablePassword("fritz", "AA2"); err == nil || err.Error() != "Password is too similar to the common dictionary password aaa (66% match); Password must be at least 8 characters long; Password must contain a lower case character; Password must contain a special character" {
		t.Error(err)
		return
	}

	if err := eud.IsAcceptablePassword("fritz", "Test1234#"); err == nil || err.Error() != "Password is too similar to the common dictionary password test12345 (88% match)" {
		t.Error(err)
		return
	}

	if err := eud.IsAcceptablePassword("fritz", "#Test1234"); err == nil || err.Error() != "Password is too similar to the common dictionary password test1234 (88% match)" {
		t.Error(err)
		return
	}

	// Test EvalPasswordStrength

	if score, warn, err := eud.EvalPasswordStrength("fritz", "aaa"); fmt.Sprintf("%v#%v#%v", score, warn, err) != "0#[]#Password matches a common dictionary password; Password must be at least 8 characters long; Password must contain an upper case character; Password must contain a number; Password must contain a special character; Password must not contain a same character sequence" {
		t.Error("Unexpected result:", fmt.Sprintf("%v#%v#%v", score, warn, err))
		return
	}

	if score, warn, err := eud.EvalPasswordStrength("fritz", "#Secr3ttest"); fmt.Sprintf("%v#%v#%v", score, warn, err) != "1#[Password should be at least 12 characters long Password should contain at least 2 upper case characters Password should contain at least 2 numbers Password should contain at least 2 special characters Password is vaguely similar to the common dictionary password secre (45% match)]#<nil>" {
		t.Error("Unexpected result:", fmt.Sprintf("%v#%v#%v", score, warn, err))
		return
	}

	if score, warn, err := eud.EvalPasswordStrength("fritz", "#SECR3TTEsT"); fmt.Sprintf("%v#%v#%v", score, warn, err) != "1#[Password should be at least 12 characters long Password should contain at least 2 lower case characters Password should contain at least 2 numbers Password should contain at least 2 special characters Password is vaguely similar to the common dictionary password secre (45% match)]#<nil>" {
		t.Error("Unexpected result:", fmt.Sprintf("%v#%v#%v", score, warn, err))
		return
	}

	if score, warn, err := eud.EvalPasswordStrength("fritz", "#ArchBoo0815!"); fmt.Sprintf("%v#%v#%v", score, warn, err) != "10#[]#<nil>" {
		t.Error("Unexpected result:", fmt.Sprintf("%v#%v#%v", score, warn, err))
		return
	}
}

func TestDictPasswordDetection(t *testing.T) {

	// No match

	match, word, dist := CheckForDictPassword("ZYxzzyxzzy55xz#")

	if res := fmt.Sprintf("%v#%v#%v", match, word, dist); res != "false##-1" {
		t.Error("Unexpected result:", res)
		return
	}

	// Direct match

	match, word, dist = CheckForDictPassword("fireball")

	if res := fmt.Sprintf("%v#%v#%v", match, word, dist); res != "true#fireball#0" {
		t.Error("Unexpected result:", res)
		return
	}

	// Partial match

	match, word, dist = CheckForDictPassword("testfire")

	if res := fmt.Sprintf("%v#%v#%v", match, word, dist); res != "false#testibil#4" {
		t.Error("Unexpected result:", res)
		return
	}

	match, word, dist = CheckForDictPassword("tuberbla")

	if res := fmt.Sprintf("%v#%v#%v", match, word, dist); res != "false#erbol#5" {
		t.Error("Unexpected result:", res)
		return
	}
}
