package command

import (
	"context"
	"fmt"

	"github.com/y4mas1ro/event-sourcing-study/internal/domain/account"
	"github.com/y4mas1ro/event-sourcing-study/internal/eventstore"
)

// Handler はコマンドを受け取り、集約を操作し、イベントをストアに保存する（書き込み側）。
// NATS への発行は Outbox リレーが担当するため、Handler は発行を行わない。
type Handler struct {
	store eventstore.EventStore
}

func NewHandler(store eventstore.EventStore) *Handler {
	return &Handler{store: store}
}

func (h *Handler) HandleOpenAccount(ctx context.Context, cmd account.OpenAccount) error {
	a := account.NewAccount()
	if err := a.Open(cmd); err != nil {
		return err
	}
	return h.save(ctx, cmd.AccountID, a, 0)
}

func (h *Handler) HandleDepositMoney(ctx context.Context, cmd account.DepositMoney) error {
	a, version, err := h.load(ctx, cmd.AccountID)
	if err != nil {
		return err
	}
	if err := a.Deposit(cmd); err != nil {
		return err
	}
	return h.save(ctx, cmd.AccountID, a, version)
}

func (h *Handler) HandleWithdrawMoney(ctx context.Context, cmd account.WithdrawMoney) error {
	a, version, err := h.load(ctx, cmd.AccountID)
	if err != nil {
		return err
	}
	if err := a.Withdraw(cmd); err != nil {
		return err
	}
	return h.save(ctx, cmd.AccountID, a, version)
}

// HandleTransferOut は送金元の出金のみを処理する。
// 受取口座への入金は TransferredOut イベントを受けた Saga が非同期で行う。
func (h *Handler) HandleTransferOut(ctx context.Context, cmd account.TransferMoney) error {
	from, fromVersion, err := h.load(ctx, cmd.FromAccountID)
	if err != nil {
		return err
	}
	if err := from.TransferOut(cmd); err != nil {
		return err
	}
	return h.save(ctx, cmd.FromAccountID, from, fromVersion)
}

// HandleTransferIn は受取口座への入金を処理する。Saga から非同期で呼ばれる。
func (h *Handler) HandleTransferIn(ctx context.Context, cmd account.TransferMoney) error {
	to, toVersion, err := h.load(ctx, cmd.ToAccountID)
	if err != nil {
		return err
	}
	to.TransferIn(cmd)
	return h.save(ctx, cmd.ToAccountID, to, toVersion)
}

func (h *Handler) load(ctx context.Context, id string) (*account.Account, int, error) {
	events, err := h.store.Load(ctx, id)
	if err != nil {
		return nil, 0, fmt.Errorf("load events: %w", err)
	}
	a := account.NewAccount()
	a.Reconstitute(events)
	return a, len(events), nil
}

func (h *Handler) save(ctx context.Context, id string, a *account.Account, expectedVersion int) error {
	changes := a.Changes()
	if err := h.store.Save(ctx, id, changes, expectedVersion); err != nil {
		return fmt.Errorf("save events: %w", err)
	}
	a.ClearChanges()
	return nil
}
