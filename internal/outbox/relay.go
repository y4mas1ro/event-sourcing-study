package outbox

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

// Relay は published=false のイベントを NATS に発行し、発行済みに更新する。
// イベントストアへの保存と NATS 発行の整合性を保証する（Outbox Pattern）。
type Relay struct {
	pool     *pgxpool.Pool
	nc       *nats.Conn
	interval time.Duration
}

func NewRelay(pool *pgxpool.Pool, nc *nats.Conn) *Relay {
	return &Relay{pool: pool, nc: nc, interval: time.Second}
}

// Run はポーリングループを起動する。ctx がキャンセルされると終了する。
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.publishPending(ctx); err != nil {
				log.Printf("[outbox] publish error: %v", err)
			}
		}
	}
}

func (r *Relay) publishPending(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// FOR UPDATE SKIP LOCKED: 複数インスタンスが起動しても重複発行しない
	rows, err := tx.Query(ctx,
		`SELECT id, aggregate_id, event_type, data, occurred_at, version
		 FROM events
		 WHERE published = false
		 ORDER BY id ASC
		 LIMIT 100
		 FOR UPDATE SKIP LOCKED`,
	)
	if err != nil {
		return err
	}

	type row struct {
		id          int64
		aggregateID string
		eventType   string
		dataJSON    []byte
		occurredAt  time.Time
		version     int
	}
	var rows2 []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.aggregateID, &r.eventType, &r.dataJSON, &r.occurredAt, &r.version); err != nil {
			rows.Close()
			return err
		}
		rows2 = append(rows2, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, ev := range rows2 {
		payload, err := json.Marshal(map[string]any{
			"aggregate_id": ev.aggregateID,
			"type":         ev.eventType,
			"data":         json.RawMessage(ev.dataJSON),
			"occurred_at":  ev.occurredAt.Format(time.RFC3339),
			"version":      ev.version,
		})
		if err != nil {
			return err
		}

		subject := "account." + ev.eventType
		if err := r.nc.Publish(subject, payload); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx,
			`UPDATE events SET published = true, published_at = NOW() WHERE id = $1`,
			ev.id,
		); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
