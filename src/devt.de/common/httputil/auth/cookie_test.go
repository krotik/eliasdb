package auth

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"devt.de/common/httputil"
	"devt.de/common/httputil/user"
)

func TestCookieAuth(t *testing.T) {

	// Set a very fast session expiry

	user.UserSessionManager.Provider.(*user.MemorySessionProvider).SetExpiry(1)

	// Create a wrapper for basic auth

	ca := NewCookieAuthHandleFuncWrapper(func(pattern string,
		handler func(http.ResponseWriter, *http.Request)) {

		// Ignore the pattern and just replace the wrappedHandleFunction

		wrappedHandleFunction = handler
	})

	ca.SetExpiry(42)

	if res := ca.Expiry(); res != 42 {
		t.Error("Unexpected result:", res)
		return
	}

	// Ensure custom handle function is set back

	defer func() { handleCallback = func(w http.ResponseWriter, r *http.Request) {} }()

	// Wrap the originalHandleFunction and let the previous code set it
	// as wrappedHandleFunction

	ca.HandleFunc("/", originalHandleFunction)

	// Test that basic authentication is active

	res, _ := sendTestRequest(TESTQUERYURL, "GET", nil, nil, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test disabling authentication temporarily

	TestCookieAuthDisabled = true

	res, _ = sendTestRequest(TESTQUERYURL, "GET", nil, nil, nil)

	if res != "Content" {
		t.Error("Unexpected result:", res)
		return
	}

	TestCookieAuthDisabled = false

	res, _ = sendTestRequest(TESTQUERYURL, "GET", nil, nil, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	// Register credentials and try to authenticate

	ca.SetAuthFunc(func(user, pass string) bool {
		return user == "yams" && pass == "yams"
	})

	// Test authentication

	if testres := ca.AuthUser("yams", "yams", true); testres != "ok" {
		t.Error("Unexpected result:", testres)
	}

	ca.AddPublicPage("/foo/pic", httputil.SingleFileServer("test.jpg", nil).ServeHTTP)

	// Simulate authentication

	ca.AddPublicPage("/foo/login", func(w http.ResponseWriter, r *http.Request) {

		// Create a token

		token := ca.AuthUser(r.Header.Get("user1"), r.Header.Get("pass1"), false)

		// Set the cookie

		ca.SetAuthCookie(token, w)
	})

	ca.AddPublicPage("/foo/logout", func(w http.ResponseWriter, r *http.Request) {
		ca.InvalidateAuthCookie(r)
		ca.RemoveAuthCookie(w)
	})

	// Get some public content

	res, resp := sendTestRequest(TESTQUERYURL+"/pic", "GET", nil, nil, nil)

	if res != "testpic" {
		t.Error("Unexpected result:", res)
		return
	}

	// Login request

	res, resp = sendTestRequest(TESTQUERYURL+"/login", "GET", map[string]string{
		"user1": "yams",
		"pass1": "yams",
	}, nil, nil)

	// Send first request which creates a session

	res, resp = sendTestRequest(TESTQUERYURL, "GET", nil, resp.Cookies(), nil)

	if res != "Content - User session: yams" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test access denied

	ca.SetAccessFunc(func(w http.ResponseWriter, r *http.Request, user string) bool {

		if strings.HasPrefix(r.URL.Path, "/foo/bar") {
			http.Error(w, "Page is restricted", http.StatusForbidden)
			return false
		}
		return true
	})

	res, resp = sendTestRequest(TESTQUERYURL+"/bar", "GET", nil, resp.Cookies(), nil)

	if res != "Page is restricted" {
		t.Error("Unexpected result:", res)
		return
	}

	// Check we have a valid session

	cookies := resp.Cookies()
	sessions, _ := user.UserSessionManager.Provider.GetAll()

	if len(sessions) != 1 {
		t.Error("Unexpected number of active sessions:", sessions)
		return
	}

	if user, ok := ca.CheckAuth(resp.Request); !ok || user != "yams" {
		t.Error("Unexpected result:", ok, user)
		return
	}

	var theSession user.Session
	for _, v := range sessions {
		theSession = v.(user.Session)
		break
	}

	var theAuth string
	for k := range ca.tokenMap.GetAll() {
		theAuth = k
		break
	}

	if len(cookies) != 2 ||
		cookies[0].Raw != fmt.Sprintf("~sid=%v; Path=/; Max-Age=%v; HttpOnly",
			theSession.ID(), CookieMaxLifetime) ||
		cookies[1].Raw != fmt.Sprintf("~aid=%v; Path=/; Max-Age=42; HttpOnly", theAuth) {

		t.Error("Unexpected cookie:", cookies)
		return
	}

	// Test session expiry

	user.UserSessionManager.Provider.Destroy(theSession.ID())

	res, _ = sendTestRequest(TESTQUERYURL, "GET", nil, cookies, nil)

	if res != "Session expired" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test a logout

	_, resp2 := sendTestRequest(TESTQUERYURL+"/logout", "GET", nil, resp.Cookies(), nil)

	cookies = resp2.Cookies()

	if len(cookies) != 1 ||
		cookies[0].Raw != "~aid=; Path=/; Max-Age=0; HttpOnly" {

		t.Error("Unexpected cookie:", cookies)
		return
	}

	cookies = resp.Cookies()

	// The next request will no longer have access to a session

	res, resp = sendTestRequest(TESTQUERYURL, "GET", nil, cookies, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	cookies = resp.Cookies()

	if len(cookies) != 0 {
		t.Error("Unexpected cookie:", cookies)
		return
	}

	// Test error cases

	// Wrong credentials - error message depends on custom handler

	res, resp = sendTestRequest(TESTQUERYURL+"/login", "GET", map[string]string{
		"user1": "yams",
		"pass1": "yams1",
	}, nil, nil)

	cookies = resp.Cookies()

	if len(cookies) != 0 {
		t.Error("Unexpected cookie:", cookies)
		return
	}

}
