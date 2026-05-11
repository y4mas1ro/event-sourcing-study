package saga

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// State は Saga の冪等性を PostgreSQL で管理する。
// 同じイベントが複数回 NATS から届いても、一度だけ処理されることを保証する。
type State struct {
	pool *pgxpool.Pool
}

func NewState(pool *pgxpool.Pool) *State {
	return &State{pool: pool}
}

// IsProcessed は指定した Saga がすでに処理済みかを確認する
func (s *State) IsProcessed(ctx context.Context, sagaType, aggregateID string, version int) (bool, error) {
	var dummy int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM saga_state WHERE saga_type=$1 AND aggregate_id=$2 AND version=$3`,
		sagaType, aggregateID, version,
	).Scan(&dummy)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// MarkProcessed は指定した Saga を処理済みとして記録する
func (s *State) MarkProcessed(ctx context.Context, sagaType, aggregateID string, version int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO saga_state (saga_type, aggregate_id, version) VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING`,
		sagaType, aggregateID, version,
	)
	return err
}
