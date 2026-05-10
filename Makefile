.PHONY: up down build run dev-nats

# NATS だけ起動（ローカル開発用）
dev-nats:
	docker compose up nats -d

# API をローカルで直接起動（NATS は別途起動しておく）
run:
	go run ./cmd/api

# ビルド確認
build:
	go build ./...

# 全サービスを Docker Compose で起動
up:
	docker compose up --build

down:
	docker compose down

# 動作確認用のサンプルリクエスト
demo:
	@echo "=== 口座開設 ==="
	curl -s -X POST http://localhost:8080/accounts \
	  -H "Content-Type: application/json" \
	  -d '{"account_id":"acc-001","owner_name":"山田太郎","initial_balance":10000}' | cat

	@echo "\n=== 入金 ==="
	curl -s -X POST http://localhost:8080/accounts/acc-001/deposits \
	  -H "Content-Type: application/json" \
	  -d '{"amount":5000}' | cat

	@echo "\n=== 出金 ==="
	curl -s -X POST http://localhost:8080/accounts/acc-001/withdrawals \
	  -H "Content-Type: application/json" \
	  -d '{"amount":3000}' | cat

	@echo "\n=== 残高照会 (読み取りモデルから) ==="
	curl -s http://localhost:8080/accounts/acc-001 | cat

	@echo ""
