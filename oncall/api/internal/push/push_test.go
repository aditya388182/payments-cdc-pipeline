package push

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
)

func TestFakeWritesJSONLines(t *testing.T) {
	p := filepath.Join(t.TempDir(), "push.jsonl")
	f := NewFake(p)
	if err := f.Send(context.Background(), alerts.Notification{Tokens: []string{"a", "b"}, Title: "ALERT: lag", Body: "x", Data: map[string]string{"state": "firing"}}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	var line map[string]any
	if err := json.Unmarshal(b, &line); err != nil || line["title"] != "ALERT: lag" || line["devices"].(float64) != 2 {
		t.Fatalf("line = %s err=%v", b, err)
	}
	if len(f.Sent()) != 1 {
		t.Fatal("not recorded in memory")
	}
}

func TestExpoBatchesAndReportsTicketErrorsWithoutLeakingTokens(t *testing.T) {
	var batches []int
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		var msgs []expoMessage
		_ = json.NewDecoder(r.Body).Decode(&msgs)
		batches = append(batches, len(msgs))
		tickets := make([]map[string]any, len(msgs))
		for i, m := range msgs {
			if strings.HasSuffix(m.To, "bad]") {
				tickets[i] = map[string]any{"status": "error", "message": "not registered", "details": map[string]string{"error": "DeviceNotRegistered"}}
			} else {
				tickets[i] = map[string]any{"status": "ok", "id": "x"}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": tickets})
	}))
	defer srv.Close()
	e := &Expo{URL: srv.URL, AccessToken: "secret-token"}
	tokens := make([]string, 0, 230)
	for i := 0; i < 229; i++ {
		tokens = append(tokens, "ExponentPushToken[ok"+string(rune('a'+i%26))+"]")
	}
	tokens = append(tokens, "ExponentPushToken[bad]")
	err := e.Send(context.Background(), alerts.Notification{Tokens: tokens, Title: "t", Body: "b"})
	if len(batches) != 3 || batches[0] != 100 || batches[2] != 30 {
		t.Fatalf("batches = %v, want 100,100,30", batches)
	}
	if auth != "Bearer secret-token" {
		t.Fatalf("auth header = %q", auth)
	}
	if err == nil || !strings.Contains(err.Error(), "DeviceNotRegistered") || strings.Contains(err.Error(), "ExponentPushToken[bad]") {
		t.Fatalf("err = %v (must name the error but not the full token)", err)
	}
}

func TestExpoHTTPErrorIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	if err := (&Expo{URL: srv.URL}).Send(context.Background(), alerts.Notification{Tokens: []string{"t"}}); err == nil {
		t.Fatal("expected an error")
	}
}
