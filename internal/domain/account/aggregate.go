package account

import (
	"errors"
	"time"
)

var (
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrNegativeAmount       = errors.New("amount must be positive")
	ErrNegativeInitialBalance = errors.New("initial balance must be non-negative")
)

// Account は銀行口座の集約。状態はイベントの再適用によってのみ構築される
type Account struct {
	id      string
	owner   string
	balance int64
	version int

	// 未コミットのイベント（コマンド処理で生成され、永続化後にクリアされる）
	changes []Event
}

func NewAccount() *Account {
	return &Account{}
}

// ID は集約IDを返す
func (a *Account) ID() string { return a.id }

// Version は現在のバージョンを返す
func (a *Account) Version() int { return a.version }

// Balance は現在の残高を返す
func (a *Account) Balance() int64 { return a.balance }

// Changes は未コミットのイベントを返す
func (a *Account) Changes() []Event { return a.changes }

// ClearChanges は未コミットイベントをクリアする（永続化後に呼ぶ）
func (a *Account) ClearChanges() { a.changes = nil }

// Open は OpenAccount コマンドを処理し AccountOpened イベントを生成する
func (a *Account) Open(cmd OpenAccount) error {
	if cmd.InitialBalance < 0 {
		return ErrNegativeInitialBalance
	}
	a.apply(Event{
		AggregateID: cmd.AccountID,
		Type:        EventAccountOpened,
		Data:        AccountOpenedData{OwnerName: cmd.OwnerName, InitialBalance: cmd.InitialBalance},
		OccurredAt:  time.Now(),
		Version:     a.version + 1,
	}, true)
	return nil
}

// Deposit は DepositMoney コマンドを処理し MoneyDeposited イベントを生成する
func (a *Account) Deposit(cmd DepositMoney) error {
	if cmd.Amount <= 0 {
		return ErrNegativeAmount
	}
	a.apply(Event{
		AggregateID: a.id,
		Type:        EventMoneyDeposited,
		Data:        MoneyDepositedData{Amount: cmd.Amount},
		OccurredAt:  time.Now(),
		Version:     a.version + 1,
	}, true)
	return nil
}

// Withdraw は WithdrawMoney コマンドを処理し MoneyWithdrawn イベントを生成する
func (a *Account) Withdraw(cmd WithdrawMoney) error {
	if cmd.Amount <= 0 {
		return ErrNegativeAmount
	}
	if a.balance < cmd.Amount {
		return ErrInsufficientFunds
	}
	a.apply(Event{
		AggregateID: a.id,
		Type:        EventMoneyWithdrawn,
		Data:        MoneyWithdrawnData{Amount: cmd.Amount},
		OccurredAt:  time.Now(),
		Version:     a.version + 1,
	}, true)
	return nil
}

// Reconstitute はイベント履歴から集約を再構築する（Event Sourcing の核心）
func (a *Account) Reconstitute(events []Event) {
	for _, e := range events {
		a.apply(e, false)
	}
}

// apply はイベントを集約に適用する。new=true の場合は changes に追加する
func (a *Account) apply(e Event, new bool) {
	switch e.Type {
	case EventAccountOpened:
		d := e.Data.(AccountOpenedData)
		a.id = e.AggregateID
		a.owner = d.OwnerName
		a.balance = d.InitialBalance
	case EventMoneyDeposited:
		d := e.Data.(MoneyDepositedData)
		a.balance += d.Amount
	case EventMoneyWithdrawn:
		d := e.Data.(MoneyWithdrawnData)
		a.balance -= d.Amount
	}
	a.version = e.Version
	if new {
		a.changes = append(a.changes, e)
	}
}
