package server

import (
	"context"
	"strconv"
	"sync"
)

// MemoryStore is an in-process SettingsStore — a map under a mutex.
// Used by tests that construct a Runtime without an open DB. Not
// persistent; values vanish on process exit.
type MemoryStore struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{m: map[string]string{}}
}

func (s *MemoryStore) Get(_ context.Context, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	if !ok {
		return "", nil
	}
	return v, nil
}

func (s *MemoryStore) GetStringOr(_ context.Context, key, dflt string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.m[key]; ok {
		return v, nil
	}
	return dflt, nil
}

func (s *MemoryStore) GetIntOr(_ context.Context, key string, dflt int) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.m[key]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	return dflt, nil
}

func (s *MemoryStore) GetFloatOr(_ context.Context, key string, dflt float64) (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.m[key]; ok {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	}
	return dflt, nil
}

func (s *MemoryStore) GetBoolOr(_ context.Context, key string, dflt bool) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.m[key]; ok {
		return v == "1" || v == "true", nil
	}
	return dflt, nil
}

func (s *MemoryStore) Set(_ context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

func (s *MemoryStore) SetInt(ctx context.Context, key string, v int) error {
	return s.Set(ctx, key, strconv.Itoa(v))
}

func (s *MemoryStore) SetFloat(ctx context.Context, key string, v float64) error {
	return s.Set(ctx, key, strconv.FormatFloat(v, 'g', -1, 64))
}

func (s *MemoryStore) SetBool(ctx context.Context, key string, v bool) error {
	if v {
		return s.Set(ctx, key, "1")
	}
	return s.Set(ctx, key, "0")
}
