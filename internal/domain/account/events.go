package account

import "time"

type EventType string

const (
	EventAccountOpened   EventType = "account.opened"
	EventMoneyDeposited  EventType = "account.money_deposited"
	EventMoneyWithdrawn  EventType = "account.money_withdrawn"
)

// Event はイベントストアに保存される不変の事実を表す
type Event struct {
	AggregateID string
	Type        EventType
	Data        any
	OccurredAt  time.Time
	Version     int
}

type AccountOpenedData struct {
	OwnerName      string
	InitialBalance int64
}

type MoneyDepositedData struct {
	Amount int64
}

type MoneyWithdrawnData struct {
	Amount int64
}
