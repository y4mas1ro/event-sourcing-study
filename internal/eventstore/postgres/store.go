package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
	"github.com/y4mas1ro/event-sourcing-study/internal/eventstore"
)

// Store は PostgreSQL を使ったイベントストア実装
type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// Pool は内部の pgxpool.Pool を返す（Outbox リレーや Saga 状態管理で使用）
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Save はトランザクション内で楽観的ロックを使ってイベントを保存する。
// published=false で保存し、Outbox リレーが NATS へ発行する。
func (s *Store) Save(ctx context.Context, aggregateID string, events []account.Event, expectedVersion int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var currentCount int
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE aggregate_id = $1`,
		aggregateID,
	).Scan(&currentCount); err != nil {
		return err
	}
	if currentCount != expectedVersion {
		return eventstore.ErrConcurrencyConflict
	}

	for _, e := range events {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO events (aggregate_id, event_type, data, occurred_at, version, published)
			 VALUES ($1, $2, $3, $4, $5, false)`,
			aggregateID, string(e.Type), data, e.OccurredAt, e.Version,
		); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// Load は集約のイベント履歴を全件取得する
func (s *Store) Load(ctx context.Context, aggregateID string) ([]account.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_type, data, occurred_at, version FROM events
		 WHERE aggregate_id = $1 ORDER BY version ASC`,
		aggregateID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []account.Event
	for rows.Next() {
		var (
			eventType  string
			dataJSON   []byte
			occurredAt time.Time
			version    int
		)
		if err := rows.Scan(&eventType, &dataJSON, &occurredAt, &version); err != nil {
			return nil, err
		}

		data, err := deserialize(account.EventType(eventType), dataJSON)
		if err != nil {
			return nil, err
		}

		events = append(events, account.Event{
			AggregateID: aggregateID,
			Type:        account.EventType(eventType),
			Data:        data,
			OccurredAt:  occurredAt,
			Version:     version,
		})
	}
	return events, rows.Err()
}

func deserialize(eventType account.EventType, data []byte) (any, error) {
	switch eventType {
	case account.EventAccountOpened:
		var d account.AccountOpenedData
		return d, json.Unmarshal(data, &d)
	case account.EventMoneyDeposited:
		var d account.MoneyDepositedData
		return d, json.Unmarshal(data, &d)
	case account.EventMoneyWithdrawn:
		var d account.MoneyWithdrawnData
		return d, json.Unmarshal(data, &d)
	case account.EventMoneyTransferredOut:
		var d account.MoneyTransferredOutData
		return d, json.Unmarshal(data, &d)
	case account.EventMoneyTransferredIn:
		var d account.MoneyTransferredInData
		return d, json.Unmarshal(data, &d)
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}
