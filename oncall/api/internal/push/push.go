// Package push delivers notifications: a fake sender (JSON lines on disk, for tests and the demo) and the
// Expo push service sender.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
)

// Fake records every notification in memory and appends it to a JSONL file when a path is set.
type Fake struct {
	mu   sync.Mutex
	path string
	sent []alerts.Notification
}

func NewFake(path string) *Fake { return &Fake{path: path} }

func (f *Fake) Send(_ context.Context, n alerts.Notification) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, n)
	if f.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return err
	}
	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	line, _ := json.Marshal(map[string]any{"at": time.Now().UTC().Format(time.RFC3339), "title": n.Title, "body": n.Body, "data": n.Data, "devices": len(n.Tokens)})
	_, err = fh.Write(append(line, '\n'))
	return err
}

func (f *Fake) Sent() []alerts.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]alerts.Notification(nil), f.sent...)
}

// Expo sends through https://exp.host/--/api/v2/push/send (at most 100 messages per request).
type Expo struct {
	URL         string
	AccessToken string // optional unless the Expo project enabled enhanced push security
	HTTP        *http.Client
}

const expoMaxBatch = 100

type expoMessage struct {
	To    string            `json:"to"`
	Title string            `json:"title"`
	Body  string            `json:"body"`
	Data  map[string]string `json:"data,omitempty"`
	Sound string            `json:"sound,omitempty"`
}

type expoTicket struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Details struct {
		Error string `json:"error"`
	} `json:"details"`
}

func (e *Expo) Send(ctx context.Context, n alerts.Notification) error {
	url := e.URL
	if url == "" {
		url = "https://exp.host/--/api/v2/push/send"
	}
	hc := e.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	var failures []string
	for start := 0; start < len(n.Tokens); start += expoMaxBatch {
		end := min(start+expoMaxBatch, len(n.Tokens))
		msgs := make([]expoMessage, 0, end-start)
		for _, t := range n.Tokens[start:end] {
			msgs = append(msgs, expoMessage{To: t, Title: n.Title, Body: n.Body, Data: n.Data, Sound: "default"})
		}
		body, _ := json.Marshal(msgs)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		if e.AccessToken != "" {
			req.Header.Set("Authorization", "Bearer "+e.AccessToken)
		}
		resp, err := hc.Do(req)
		if err != nil {
			return fmt.Errorf("expo push request: %w", err)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("expo push: HTTP %d", resp.StatusCode)
		}
		var out struct {
			Data []expoTicket `json:"data"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return fmt.Errorf("expo push: bad response: %w", err)
		}
		for i, tk := range out.Data {
			if tk.Status != "ok" {
				failures = append(failures, fmt.Sprintf("%s: %s %s", redact(n.Tokens[start+i]), tk.Details.Error, tk.Message))
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("expo push: %d ticket errors: %s", len(failures), strings.Join(failures, "; "))
	}
	return nil
}

// redact keeps logs free of full device tokens.
func redact(t string) string {
	if len(t) <= 12 {
		return "***"
	}
	return t[:12] + "***"
}
