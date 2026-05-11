package projection

import (
	"sync"
	"time"
)

// AccountView は読み取り専用のフラットな口座ビュー（クエリ側）
type AccountView struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner"`
	Balance   int64     `json:"balance"`
	UpdatedAt time.Time `json:"updated_at"`
}

// AccountReadModel は AccountView を保持するインメモリ読み取りモデル
type AccountReadModel struct {
	mu   sync.RWMutex
	data map[string]*AccountView
}

// AccountReadModel は ViewReader インターフェースを実装する（インメモリ）
var _ ViewReader = (*AccountReadModel)(nil)

func NewAccountReadModel() *AccountReadModel {
	return &AccountReadModel{data: make(map[string]*AccountView)}
}

func (m *AccountReadModel) Get(id string) (*AccountView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[id]
	if !ok {
		return nil, false
	}
	copy := *v
	return &copy, true
}

func (m *AccountReadModel) Apply(id string, fn func(*AccountView)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[id]
	if !ok {
		v = &AccountView{ID: id}
		m.data[id] = v
	}
	fn(v)
	v.UpdatedAt = time.Now()
}
