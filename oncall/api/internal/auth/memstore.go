package auth

import (
	"context"
	"sync"
	"time"
)

// MemStore is an in-memory Store for tests; it has the same atomic semantics as the Postgres one.
type MemStore struct {
	mu      sync.Mutex
	states  map[string]StateData
	codes   map[string]*memCode
	refresh map[string]*memRefresh // by token hash
}

type memCode struct {
	CodeData
	used bool
}
type memRefresh struct {
	RefreshRecord
	used, revoked bool
}

func NewMemStore() *MemStore {
	return &MemStore{states: map[string]StateData{}, codes: map[string]*memCode{}, refresh: map[string]*memRefresh{}}
}

func (m *MemStore) SaveState(_ context.Context, h string, s StateData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[h] = s
	return nil
}

func (m *MemStore) ConsumeState(_ context.Context, h string, now time.Time) (StateData, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.states[h]
	delete(m.states, h)
	if !ok || !s.ExpiresAt.After(now) {
		return StateData{}, false, nil
	}
	return s, true, nil
}

func (m *MemStore) SaveCode(_ context.Context, h string, c CodeData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.codes[h] = &memCode{CodeData: c}
	return nil
}

func (m *MemStore) ConsumeCode(_ context.Context, h string, now time.Time) (CodeData, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.codes[h]
	if !ok || c.used || !c.ExpiresAt.After(now) {
		return CodeData{}, false, nil
	}
	c.used = true
	return c.CodeData, true, nil
}

func (m *MemStore) CreateRefresh(_ context.Context, r RefreshRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refresh[r.TokenHash] = &memRefresh{RefreshRecord: r}
	return nil
}

func (m *MemStore) RotateRefresh(_ context.Context, presented string, next RefreshRecord, now time.Time) (RefreshRecord, RotateResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.refresh[presented]
	if !ok {
		return RefreshRecord{}, NotFound, nil
	}
	if old.used {
		for _, r := range m.refresh {
			if r.FamilyID == old.FamilyID {
				r.revoked = true
			}
		}
		return old.RefreshRecord, Reused, nil
	}
	if old.revoked {
		return old.RefreshRecord, Revoked, nil
	}
	if !old.ExpiresAt.After(now) {
		return old.RefreshRecord, Expired, nil
	}
	old.used = true
	next.FamilyID, next.Login, next.Role = old.FamilyID, old.Login, old.Role
	m.refresh[next.TokenHash] = &memRefresh{RefreshRecord: next}
	return old.RefreshRecord, Rotated, nil
}

func (m *MemStore) RevokeFamily(_ context.Context, fam string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.refresh {
		if r.FamilyID == fam {
			r.revoked = true
		}
	}
	return nil
}

// Live counts refresh tokens in a family that are neither rotated nor revoked (tests).
func (m *MemStore) Live(fam string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, r := range m.refresh {
		if r.FamilyID == fam && !r.used && !r.revoked {
			n++
		}
	}
	return n
}

// MemAuditor collects audit events (tests).
type MemAuditor struct {
	mu     sync.Mutex
	Events []AuditEvent
}

func (a *MemAuditor) Audit(_ context.Context, e AuditEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Events = append(a.Events, e)
}

func (a *MemAuditor) Count(action, result string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, e := range a.Events {
		if e.Action == action && (result == "" || e.Result == result) {
			n++
		}
	}
	return n
}
