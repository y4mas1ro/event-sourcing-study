package eventstore

import (
	"context"
	"errors"
	"sync"

	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
)

var ErrConcurrencyConflict = errors.New("concurrency conflict: version mismatch")

// EventStore はイベントの保存と読み込みを抽象化するインターフェース
type EventStore interface {
	Save(ctx context.Context, aggregateID string, events []account.Event, expectedVersion int) error
	Load(ctx context.Context, aggregateID string) ([]account.Event, error)
}

// MemoryStore はインメモリのイベントストア実装（開発・テスト用）
type MemoryStore struct {
	mu     sync.RWMutex
	events map[string][]account.Event
}

func New() *MemoryStore {
	return &MemoryStore{events: make(map[string][]account.Event)}
}

func (s *MemoryStore) Save(_ context.Context, aggregateID string, events []account.Event, expectedVersion int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.events[aggregateID]
	if len(existing) != expectedVersion {
		return ErrConcurrencyConflict
	}
	s.events[aggregateID] = append(existing, events...)
	return nil
}

func (s *MemoryStore) Load(_ context.Context, aggregateID string) ([]account.Event, error) {
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
