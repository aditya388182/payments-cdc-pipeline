package alerts

import (
	"context"
	"sort"
	"sync"
)

// MemStore is an in-memory Store for tests and local experiments.
type MemStore struct {
	mu     sync.Mutex
	nextID int64
	byID   map[int64]Alert
}

func NewMemStore() *MemStore { return &MemStore{byID: map[int64]Alert{}} }

func (m *MemStore) Active(_ context.Context) ([]Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Alert
	for _, a := range m.byID {
		if a.State == StateFiring || a.State == StateAcknowledged {
			out = append(out, a)
		}
	}
	return out, nil
}

func (m *MemStore) Create(_ context.Context, a Alert) (Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	a.ID = m.nextID
	m.byID[a.ID] = a
	return a, nil
}

func (m *MemStore) Update(_ context.Context, a Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byID[a.ID]; !ok {
		return ErrNotFound
	}
	m.byID[a.ID] = a
	return nil
}

func (m *MemStore) Get(_ context.Context, id int64) (Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.byID[id]
	if !ok {
		return Alert{}, ErrNotFound
	}
	return a, nil
}

func (m *MemStore) List(_ context.Context, states []string, limit int) ([]Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	want := map[string]bool{}
	for _, s := range states {
		want[s] = true
	}
	var out []Alert
	for _, a := range m.byID {
		if len(want) == 0 || want[a.State] {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
