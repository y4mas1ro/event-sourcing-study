package account

// OpenAccount は新しい口座を開設するコマンド
type OpenAccount struct {
	AccountID      string
	OwnerName      string
	InitialBalance int64
}

// DepositMoney は口座に入金するコマンド
type DepositMoney struct {
	AccountID string
	Amount    int64
}

// WithdrawMoney は口座から出金するコマンド
type WithdrawMoney struct {
	AccountID string
	Amount    int64
}
