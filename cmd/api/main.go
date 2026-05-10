package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/y4mas1ro/event-sourcing-study/internal/command"
	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
	"github.com/y4mas1ro/event-sourcing-study/internal/eventstore"
	"github.com/y4mas1ro/event-sourcing-study/internal/projection"
	"github.com/y4mas1ro/event-sourcing-study/internal/query"
)

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

	store := eventstore.New()
	model := projection.NewAccountReadModel()

	// このプロセス内でもプロジェクションを購読する（単一プロセス構成）
	subscribeProjection(nc, model)

	cmdHandler := command.NewHandler(store, nc)
	qryHandler := query.NewHandler(model)

	mux := http.NewServeMux()

	// --- コマンド側 (書き込み) ---
	mux.HandleFunc("POST /accounts", handleOpenAccount(cmdHandler))
	mux.HandleFunc("POST /accounts/{id}/deposits", handleDeposit(cmdHandler))
	mux.HandleFunc("POST /accounts/{id}/withdrawals", handleWithdraw(cmdHandler))

	mux.HandleFunc("POST /accounts/{id}/transfers", handleTransfer(cmdHandler))

	// --- クエリ側 (読み取り) ---
	mux.HandleFunc("GET /accounts/{id}", handleGetAccount(qryHandler))

	addr := ":8080"
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("API server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("API server stopped")
}

func subscribeProjection(nc *nats.Conn, model *projection.AccountReadModel) {
	_, _ = nc.Subscribe("account.*", func(msg *nats.Msg) {
		var e struct {
			AggregateID string          `json:"aggregate_id"`
			Type        string          `json:"type"`
			Data        json.RawMessage `json:"data"`
		}
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

// --- ハンドラー ---

func handleOpenAccount(h *command.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AccountID      string `json:"account_id"`
			OwnerName      string `json:"owner_name"`
			InitialBalance int64  `json:"initial_balance"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cmd := account.OpenAccount{
			AccountID:      body.AccountID,
			OwnerName:      body.OwnerName,
			InitialBalance: body.InitialBalance,
		}
		if err := h.HandleOpenAccount(r.Context(), cmd); err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		log.Printf("[cmd] OpenAccount: %s (owner=%s, balance=%d)", cmd.AccountID, cmd.OwnerName, cmd.InitialBalance)
		w.WriteHeader(http.StatusCreated)
	}
}

func handleDeposit(h *command.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Amount int64 `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cmd := account.DepositMoney{
			AccountID: r.PathValue("id"),
			Amount:    body.Amount,
		}
		if err := h.HandleDepositMoney(r.Context(), cmd); err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		log.Printf("[cmd] DepositMoney: %s (+%d)", cmd.AccountID, cmd.Amount)
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleWithdraw(h *command.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Amount int64 `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cmd := account.WithdrawMoney{
			AccountID: r.PathValue("id"),
			Amount:    body.Amount,
		}
		if err := h.HandleWithdrawMoney(r.Context(), cmd); err != nil {
			code := http.StatusUnprocessableEntity
			if errors.Is(err, account.ErrInsufficientFunds) {
				code = http.StatusConflict
			}
			writeError(w, code, err.Error())
			return
		}
		log.Printf("[cmd] WithdrawMoney: %s (-%d)", cmd.AccountID, cmd.Amount)
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleTransfer(h *command.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ToAccountID string `json:"to_account_id"`
			Amount      int64  `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cmd := account.TransferMoney{
			FromAccountID: r.PathValue("id"),
			ToAccountID:   body.ToAccountID,
			Amount:        body.Amount,
		}
		if err := h.HandleTransferMoney(r.Context(), cmd); err != nil {
			code := http.StatusUnprocessableEntity
			if errors.Is(err, account.ErrInsufficientFunds) {
				code = http.StatusConflict
			}
			writeError(w, code, err.Error())
			return
		}
		log.Printf("[cmd] TransferMoney: %s -> %s (%d)", cmd.FromAccountID, cmd.ToAccountID, cmd.Amount)
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleGetAccount(h *query.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		view, err := h.GetAccount(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(view)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
