package auth

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"devt.de/common/httputil/user"
)

func TestBasicAuth(t *testing.T) {

	// Set a very fast session expiry

	user.UserSessionManager.Provider.(*user.MemorySessionProvider).SetExpiry(1)

	// Create a wrapper for basic auth

	ba := NewBashicAuthHandleFuncWrapper(func(pattern string,
		handler func(http.ResponseWriter, *http.Request)) {

		// Ignore the pattern and just replace the wrappedHandleFunction

		wrappedHandleFunction = handler
	})

	// Wrap the originalHandleFunction and let the previous code set it
	// as wrappedHandleFunction

	ba.HandleFunc("/", originalHandleFunction)

	// Test that basic authentication is active

	res, _ := sendTestRequest(TESTQUERYURL, "GET", nil, nil, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	// Register credentials and try to authenticate

	ba.SetAuthFunc(func(user, pass string) bool {
		return user == "yams" && pass == "yams"
	})

	passStr := base64.StdEncoding.EncodeToString([]byte("yams:yams"))

	res, resp := sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, nil, nil)

	if res != "Content - User session: yams" {
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

	var theSession user.Session
	for _, v := range sessions {
		theSession = v.(user.Session)
		break
	}

	if len(cookies) != 1 ||
		cookies[0].Raw != fmt.Sprintf("~sid=%v; Path=/; Max-Age=%v; HttpOnly",
			theSession.ID(), CookieMaxLifetime) {

		t.Error("Unexpected cookie:", cookies)
		return
	}

	// The next request will have access to a session

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, cookies, nil)

	if res != "Content - User session: yams" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test expiry

	time.Sleep(2 * time.Second)

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, cookies, nil)

	if res != "Session expired" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test destroying session

	res, resp = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, nil, nil)

	if res != "Content - User session: yams" {
		t.Error("Unexpected result:", res)
		return
	}

	cookies = resp.Cookies()
	sessions, _ = user.UserSessionManager.Provider.GetAll()

	if len(sessions) != 1 {
		t.Error("Unexpected number of active sessions:", sessions)
		return
	}

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, cookies, nil)

	if res != "Content - User session: yams" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test access denied

	ba.SetAccessFunc(func(w http.ResponseWriter, r *http.Request, user string) bool {

		if strings.HasPrefix(r.URL.Path, "/foo/bar") {
			http.Error(w, "Page is restricted", http.StatusForbidden)
			return false
		}
		return true
	})

	res, resp = sendTestRequest(TESTQUERYURL+"/bar", "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, cookies, nil)

	if res != "Page is restricted" {
		t.Error("Unexpected result:", res)
		return
	}

	for _, k := range sessions {
		user.UserSessionManager.Provider.Destroy(k.ID())
	}

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr,
	}, cookies, nil)

	if res != "Session expired" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test error cases

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStr + "wrong",
	}, cookies, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic" + passStr,
	}, cookies, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	passStrWrong := base64.StdEncoding.EncodeToString([]byte("yams:yams1"))

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStrWrong,
	}, cookies, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	passStrWrong = base64.StdEncoding.EncodeToString([]byte("yamsyams"))

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStrWrong,
	}, cookies, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}

	passStrWrong = base64.StdEncoding.EncodeToString([]byte("yams1:yams"))

	res, _ = sendTestRequest(TESTQUERYURL, "GET", map[string]string{
		"Authorization": "Basic " + passStrWrong,
	}, cookies, nil)

	if res != "Unauthorized" {
		t.Error("Unexpected result:", res)
		return
	}
}
