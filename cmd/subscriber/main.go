package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
	"github.com/y4mas1ro/event-sourcing-study/internal/projection"
)

// subscriber は NATS からイベントを受け取り読み取りモデルを更新するプロセス
// CQRS の「読み取り側」を担当する
func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS connect: %v", err)
	}
	defer nc.Close()

	model := projection.NewAccountReadModel()

	// account.* のすべてのイベントを購読する
	_, err = nc.Subscribe("account.*", func(msg *nats.Msg) {
		handleEvent(msg, model)
	})
	if err != nil {
		log.Fatalf("NATS subscribe: %v", err)
	}

	log.Printf("subscriber started, listening on account.*")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("subscriber shutting down")
}

type natsEvent struct {
	AggregateID string          `json:"aggregate_id"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
	OccurredAt  string          `json:"occurred_at"`
	Version     int             `json:"version"`
}

func handleEvent(msg *nats.Msg, model *projection.AccountReadModel) {
	var e natsEvent
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		log.Printf("unmarshal event: %v", err)
		return
	}

	log.Printf("[projection] received %s for %s (v%d)", e.Type, e.AggregateID, e.Version)

	switch account.EventType(e.Type) {
	case account.EventAccountOpened:
		var d account.AccountOpenedData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			log.Printf("unmarshal AccountOpenedData: %v", err)
			return
		}
		model.Apply(e.AggregateID, func(v *projection.AccountView) {
			v.Owner = d.OwnerName
			v.Balance = d.InitialBalance
		})

	case account.EventMoneyDeposited:
		var d account.MoneyDepositedData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			log.Printf("unmarshal MoneyDepositedData: %v", err)
			return
		}
		model.Apply(e.AggregateID, func(v *projection.AccountView) {
			v.Balance += d.Amount
		})

	case account.EventMoneyWithdrawn:
		var d account.MoneyWithdrawnData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			log.Printf("unmarshal MoneyWithdrawnData: %v", err)
			return
		}
		model.Apply(e.AggregateID, func(v *projection.AccountView) {
			v.Balance -= d.Amount
		})
	}
}
