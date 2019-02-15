package user

import (
	"fmt"
	"net/http"
	"testing"
)

func TestDefaultSession(t *testing.T) {

	ds := NewDefaultSession("test", "user1")

	if res := ds.ID(); res != "test" {
		t.Error("Unexpected id:", res)
		return
	}

	ds.Set("key1", "value1")

	if res, ok := ds.Get("key1"); !ok || res != "value1" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := ds.GetAll(); fmt.Sprint(res) != "map[key1:value1]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := fmt.Sprint(ds); res != "Session: test (User:user1 Values:map[key1:value1])" {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestSessionCreation(t *testing.T) {

	handleCallback = func(w http.ResponseWriter, r *http.Request) {
		param := r.URL.Query()
		name, ok := param["user"]
		if ok {

			// Register the new user

			UserSessionManager.GetSession(name[0], w, r, true)

			if hasCookie, isActive := UserSessionManager.CheckSessionCookie(r); !hasCookie || !isActive {
				t.Error("Unexpected result:", hasCookie, isActive)
				return
			}
		}

		session, _ := UserSessionManager.GetSession("", w, r, false)

		_, ok = param["logout"]
		if ok && session != nil {

			if hasCookie, isActive := UserSessionManager.CheckSessionCookie(r); !hasCookie || !isActive {
				t.Error("Unexpected result:", hasCookie, isActive)
				return
			}

			UserSessionManager.RemoveSessionCookie(w)
			UserSessionManager.Provider.Destroy(session.ID())
		}
	}

	res, resp := sendTestRequest(TESTQUERYURL+"?user=fred", "GET", nil, nil, nil)

	if res != "Content" {
		t.Error("Unexpected response:", res)
		return
	}

	// Check we have a valid session

	cookies := resp.Cookies()
	sessions, _ := UserSessionManager.Provider.GetAll()

	if len(sessions) != 1 {
		t.Error("Unexpected number of active sessions:", sessions)
		return
	}

	var theSession Session
	for _, v := range sessions {
		theSession = v.(Session)
		break
	}

	if len(cookies) != 1 ||
		cookies[0].Raw != fmt.Sprintf("~sid=%v; Path=/; Max-Age=%v; HttpOnly",
			theSession.ID(), CookieMaxLifetime) {

		t.Error("Unexpected cookie:", cookies)
		return
	}

	// The next request will have access to a session

	res, resp = sendTestRequest(TESTQUERYURL, "GET", nil, cookies, nil)

	if res != "Content - User session: fred" {
		t.Error("Unexpected result:", res)
		return
	}

	session, _ := UserSessionManager.GetSession("", nil, resp.Request, false)
	if session == nil {
		t.Error("Unexpected result")
		return
	}

	res, resp = sendTestRequest(TESTQUERYURL+"?logout=1", "GET", nil, cookies, nil)
	cookies = resp.Cookies()

	if res != "Content - User session: fred" {
		t.Error("Unexpected result:", res)
		return
	}

	if len(cookies) != 1 ||
		fmt.Sprint(cookies[0].Raw) != "~sid=; Path=/; Max-Age=0; HttpOnly" {

		t.Error("Unexpected cookie:", cookies[0])
		return
	}

	// Check the user is no longer identified in the session

	res, resp = sendTestRequest(TESTQUERYURL, "GET", nil, cookies, nil)

	if res != "Content" {
		t.Error("Unexpected result:", res)
		return
	}

	// We can still see the cookie in the session but it has now an invalid value

	if hasCookie, isActive := UserSessionManager.CheckSessionCookie(resp.Request); !hasCookie || isActive {
		t.Error("Unexpected result:", hasCookie, isActive)
		return
	}

	session, err := UserSessionManager.GetSession("", nil, resp.Request, false)
	if session != nil || err != nil {
		t.Error("Unexpected result:", err)
		return
	}

}
