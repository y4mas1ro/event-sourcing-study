package eventstore

import (
	"context"
	"errors"
	"sync"

	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
)

var ErrConcurrencyConflict = errors.New("concurrency conflict: version mismatch")

// Store はイベントをインメモリに保存するイベントストア
// 本番では PostgreSQL や EventStoreDB を使用する
type Store struct {
	mu     sync.RWMutex
	events map[string][]account.Event // aggregateID -> events
}

func New() *Store {
	return &Store{events: make(map[string][]account.Event)}
}

// Save はイベントをストアに保存する。楽観的ロックでバージョン競合を検出する
func (s *Store) Save(_ context.Context, aggregateID string, events []account.Event, expectedVersion int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.events[aggregateID]
	currentVersion := len(existing)
	if currentVersion != expectedVersion {
		return ErrConcurrencyConflict
	}

	s.events[aggregateID] = append(existing, events...)
	return nil
}

// Load は指定した集約のイベント履歴をすべて返す
func (s *Store) Load(_ context.Context, aggregateID string) ([]account.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events, ok := s.events[aggregateID]
	if !ok {
		return nil, nil
	}
	result := make([]account.Event, len(events))
	copy(result, events)
	return result, nil
}
