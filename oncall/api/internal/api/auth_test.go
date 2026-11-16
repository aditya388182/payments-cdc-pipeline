package api

import (
	"strings"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
)

func TestJWTRejectsTheNoneAlgorithmAndTampering(t *testing.T) {
	s := auth.NewSigner(strings.Repeat("k", 40), time.Minute)
	tok, _, _ := s.Issue(auth.Principal{Login: "a", Role: auth.RoleViewer})
	if _, err := s.Verify(tok); err != nil {
		t.Fatal(err)
	}
	// header {"alg":"none"} with the original payload and no signature
	parts := strings.Split(tok, ".")
	none := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0." + parts[1] + "."
	if _, err := s.Verify(none); err == nil {
		t.Fatal("alg=none was accepted")
	}
	if _, err := s.Verify(parts[0] + "." + parts[1] + ".AAAA"); err == nil {
		t.Fatal("tampered signature was accepted")
	}
	if _, _, err := s.Issue(auth.Principal{Login: "a", Role: "root"}); err == nil {
		t.Fatal("an unknown role must not be signable")
	}
}
