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
)

// subscriber は NATS からイベントを受け取り、
// プロジェクション更新と Transfer Saga を担当するプロセス
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

	store, closeStore := newEventStore(ctx)
	defer closeStore()

	model := projection.NewAccountReadModel()
	cmdHandler := command.NewHandler(store, nc)

	subscribeProjection(nc, model)
	subscribeTransferSaga(nc, cmdHandler)

	log.Println("subscriber started, listening on account.*")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("subscriber shutting down")
}

// newEventStore は DATABASE_URL が設定されていれば PostgreSQL、なければインメモリを返す
func newEventStore(ctx context.Context) (eventstore.EventStore, func()) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Println("[store] using in-memory event store")
		return eventstore.New(), func() {}
	}
	store, err := pgstore.New(ctx, dsn)
	if err != nil {
		log.Fatalf("postgres event store: %v", err)
	}
	log.Println("[store] using PostgreSQL event store")
	return store, store.Close
}

// subscribeTransferSaga は TransferredOut イベントを購読し、
// 受取口座への TransferredIn を非同期で処理する
func subscribeTransferSaga(nc *nats.Conn, h *command.Handler) {
	subject := "account." + string(account.EventMoneyTransferredOut)
	_, _ = nc.Subscribe(subject, func(msg *nats.Msg) {
		var e struct {
			AggregateID string                          `json:"aggregate_id"`
			Data        account.MoneyTransferredOutData `json:"data"`
		}
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			log.Printf("[saga] unmarshal error: %v", err)
			return
		}
		cmd := account.TransferMoney{
			FromAccountID: e.AggregateID,
			ToAccountID:   e.Data.ToAccountID,
			Amount:        e.Data.Amount,
		}
		log.Printf("[saga] TransferredOut received: %s -> %s (%d), processing TransferIn...",
			cmd.FromAccountID, cmd.ToAccountID, cmd.Amount)
		if err := h.HandleTransferIn(context.Background(), cmd); err != nil {
			log.Printf("[saga] HandleTransferIn error: %v", err)
		}
	})
}

type natsEvent struct {
	AggregateID string          `json:"aggregate_id"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
	OccurredAt  string          `json:"occurred_at"`
	Version     int             `json:"version"`
}

func subscribeProjection(nc *nats.Conn, model *projection.AccountReadModel) {
	_, _ = nc.Subscribe("account.*", func(msg *nats.Msg) {
		var e natsEvent
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			log.Printf("[projection] unmarshal: %v", err)
			return
		}

		log.Printf("[projection] received %s for %s (v%d)", e.Type, e.AggregateID, e.Version)

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
			model.Apply(e.AggregateID, func(v *projection.AccountView) {
				v.Balance += d.Amount
			})
		case account.EventMoneyWithdrawn:
			var d account.MoneyWithdrawnData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) {
				v.Balance -= d.Amount
			})
		case account.EventMoneyTransferredOut:
			var d account.MoneyTransferredOutData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) {
				v.Balance -= d.Amount
			})
		case account.EventMoneyTransferredIn:
			var d account.MoneyTransferredInData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return
			}
			model.Apply(e.AggregateID, func(v *projection.AccountView) {
				v.Balance += d.Amount
			})
		}
	})
}
