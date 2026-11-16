package api

import (
	"testing"

	"go.uber.org/goleak"
)

// The tests talk to httptest servers through http.DefaultClient, whose idle keep-alive connections are not a leak in
// the code under test; everything else must be gone when the tests finish.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"))
}
