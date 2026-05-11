package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/y4mas1ro/event-sourcing-study/internal/command"
	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
	"github.com/y4mas1ro/event-sourcing-study/internal/eventstore"
	pgstore "github.com/y4mas1ro/event-sourcing-study/internal/eventstore/postgres"
	"github.com/y4mas1ro/event-sourcing-study/internal/projection"
	"github.com/y4mas1ro/event-sourcing-study/internal/saga"
)

// subscriber は NATS からイベントを受け取り、
// Transfer Saga（冪等性保証付き）とプロジェクション更新を担当するプロセス
func main() {
	ctx := context.Background()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS connect: %v", err)
	}
	defer nc.Close()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Println("[store] using in-memory event store (memory mode)")
		runInMemoryMode(nc)
	} else {
		log.Println("[store] using PostgreSQL event store (postgres mode)")
		runPostgresMode(ctx, nc, dsn)
	}
}

// runPostgresMode は PostgreSQL を使ったフル機能モード。
// Saga 冪等性チェックと永続化された読み取りモデルを使用する。
func runPostgresMode(ctx context.Context, nc *nats.Conn, dsn string) {
	pg, err := pgstore.New(ctx, dsn)
	if err != nil {
		log.Fatalf("postgres event store: %v", err)
	}
	defer pg.Close()

	sagaState := saga.NewState(pg.Pool())
	viewStore := projection.NewPostgresViewStore(pg.Pool())
	cmdHandler := command.NewHandler(pg)

	subscribeProjectionPostgres(nc, viewStore)
	subscribeTransferSaga(nc, cmdHandler, sagaState)

	log.Println("subscriber started (postgres mode), listening on account.*")
	waitForShutdown()
}

// runInMemoryMode はインメモリモード（DATABASE_URL 未設定時のフォールバック）
func runInMemoryMode(nc *nats.Conn) {
	memStore := eventstore.New()
	model := projection.NewAccountReadModel()
	cmdHandler := command.NewHandler(memStore)

	subscribeProjectionInMemory(nc, model)
	subscribeTransferSaga(nc, cmdHandler, nil)

	log.Println("subscriber started (memory mode), listening on account.*")
	waitForShutdown()
}

func waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("subscriber shutting down")
}

// subscribeTransferSaga は TransferredOut イベントを購読し、
// 冪等性チェック後に受取口座への TransferredIn を処理する。
// sagaState が nil の場合（インメモリモード）は冪等性チェックをスキップする。
func subscribeTransferSaga(nc *nats.Conn, h *command.Handler, sagaState *saga.State) {
	subject := "account." + string(account.EventMoneyTransferredOut)
	_, _ = nc.Subscribe(subject, func(msg *nats.Msg) {
		var e struct {
			AggregateID string                          `json:"aggregate_id"`
			Data        account.MoneyTransferredOutData `json:"data"`
			Version     int                             `json:"version"`
		}
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			log.Printf("[saga] unmarshal error: %v", err)
			return
		}

		ctx := context.Background()

		// 冪等性チェック: すでに処理済みなら何もしない
		if sagaState != nil {
			processed, err := sagaState.IsProcessed(ctx, "transfer", e.AggregateID, e.Version)
			if err != nil {
				log.Printf("[saga] state check error: %v", err)
				return
			}
			if processed {
				log.Printf("[saga] already processed: %s v%d, skipping", e.AggregateID, e.Version)
				return
			}
		}

		cmd := account.TransferMoney{
			FromAccountID: e.AggregateID,
			ToAccountID:   e.Data.ToAccountID,
			Amount:        e.Data.Amount,
		}
		log.Printf("[saga] TransferredOut: %s -> %s (%d), processing TransferIn...",
			cmd.FromAccountID, cmd.ToAccountID, cmd.Amount)

		if err := h.HandleTransferIn(ctx, cmd); err != nil {
			log.Printf("[saga] HandleTransferIn error: %v", err)
			return
		}

		// 処理成功後に記録（二重処理を防ぐ）
		if sagaState != nil {
			if err := sagaState.MarkProcessed(ctx, "transfer", e.AggregateID, e.Version); err != nil {
				log.Printf("[saga] mark processed error: %v", err)
			}
		}
	})
}

type natsEvent struct {
	AggregateID string          `json:"aggregate_id"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
	Version     int             `json:"version"`
}

func subscribeProjectionPostgres(nc *nats.Conn, store *projection.PostgresViewStore) {
	_, _ = nc.Subscribe("account.*", func(msg *nats.Msg) {
		var e natsEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			log.Printf("[projection] unmarshal: %v", err)
			return
		}
		log.Printf("[projection] received %s for %s (v%d)", e.Type, e.AggregateID, e.Version)

		ctx := context.Background()
		var applyErr error

		switch account.EventType(e.Type) {
		case account.EventAccountOpened:
			var d account.AccountOpenedData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			applyErr = store.ApplyOpened(ctx, e.AggregateID, d.OwnerName, d.InitialBalance)
		case account.EventMoneyDeposited:
			var d account.MoneyDepositedData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			applyErr = store.ApplyBalanceDelta(ctx, e.AggregateID, +d.Amount)
		case account.EventMoneyWithdrawn:
			var d account.MoneyWithdrawnData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			applyErr = store.ApplyBalanceDelta(ctx, e.AggregateID, -d.Amount)
		case account.EventMoneyTransferredOut:
			var d account.MoneyTransferredOutData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			applyErr = store.ApplyBalanceDelta(ctx, e.AggregateID, -d.Amount)
		case account.EventMoneyTransferredIn:
			var d account.MoneyTransferredInData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			applyErr = store.ApplyBalanceDelta(ctx, e.AggregateID, +d.Amount)
		}

		if applyErr != nil {
			log.Printf("[projection] apply error (%s): %v", e.Type, applyErr)
		}
	})
}

func subscribeProjectionInMemory(nc *nats.Conn, model *projection.AccountReadModel) {
	_, _ = nc.Subscribe("account.*", func(msg *nats.Msg) {
		var e natsEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			return
		}
		switch account.EventType(e.Type) {
		case account.EventAccountOpened:
			var d account.AccountOpenedData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) {
				v.Owner = d.OwnerName
				v.Balance = d.InitialBalance
			})
		case account.EventMoneyDeposited:
			var d account.MoneyDepositedData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) { v.Balance += d.Amount })
		case account.EventMoneyWithdrawn:
			var d account.MoneyWithdrawnData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) { v.Balance -= d.Amount })
		case account.EventMoneyTransferredOut:
			var d account.MoneyTransferredOutData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) { v.Balance -= d.Amount })
		case account.EventMoneyTransferredIn:
			var d account.MoneyTransferredInData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) { v.Balance += d.Amount })
		}
	})
}
