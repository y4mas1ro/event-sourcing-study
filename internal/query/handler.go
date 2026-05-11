package query

import (
	"errors"

	"github.com/y4mas1ro/event-sourcing-study/internal/projection"
)

var ErrNotFound = errors.New("account not found")

// Handler はクエリを処理し読み取りモデルからデータを返す（読み取り側）
type Handler struct {
	model projection.ViewReader
}

func NewHandler(model projection.ViewReader) *Handler {
	return &Handler{model: model}
}

func (h *Handler) GetAccount(id string) (*projection.AccountView, error) {
	view, ok := h.model.Get(id)
	if !ok {
		return nil, ErrNotFound
	}
	return view, nil
}
