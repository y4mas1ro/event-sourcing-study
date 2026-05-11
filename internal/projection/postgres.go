package projection

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ViewReader は読み取りモデルの取得を抽象化するインターフェース
type ViewReader interface {
	Get(id string) (*AccountView, bool)
}

// PostgresViewStore は account_views テーブルを使った永続化読み取りモデル。
// サーバ再起動後もデータが保持される。
type PostgresViewStore struct {
	pool *pgxpool.Pool
}

func NewPostgresViewStore(pool *pgxpool.Pool) *PostgresViewStore {
	return &PostgresViewStore{pool: pool}
}

func (s *PostgresViewStore) Get(id string) (*AccountView, bool) {
	var v AccountView
	err := s.pool.QueryRow(context.Background(),
		`SELECT id, owner, balance, updated_at FROM account_views WHERE id = $1`, id,
	).Scan(&v.ID, &v.Owner, &v.Balance, &v.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	return &v, true
}

func (s *PostgresViewStore) ApplyOpened(ctx context.Context, id, owner string, balance int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_views (id, owner, balance)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE SET owner=$2, balance=$3, updated_at=NOW()`,
		id, owner, balance,
	)
	return err
}

func (s *PostgresViewStore) ApplyBalanceDelta(ctx context.Context, id string, delta int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE account_views SET balance = balance + $2, updated_at = NOW() WHERE id = $1`,
		id, delta,
	)
	return err
}
