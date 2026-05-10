.PHONY: up down build run dev-nats demo

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
	@echo "=== 口座開設 (山田太郎) ==="
	curl -s -X POST http://localhost:8080/accounts \
	  -H "Content-Type: application/json" \
	  -d '{"account_id":"acc-001","owner_name":"山田太郎","initial_balance":10000}' | cat

	@echo "\n=== 口座開設 (鈴木花子) ==="
	curl -s -X POST http://localhost:8080/accounts \
	  -H "Content-Type: application/json" \
	  -d '{"account_id":"acc-002","owner_name":"鈴木花子","initial_balance":0}' | cat

	@echo "\n=== 入金 ==="
	curl -s -X POST http://localhost:8080/accounts/acc-001/deposits \
	  -H "Content-Type: application/json" \
	  -d '{"amount":5000}' | cat

	@echo "\n=== 出金 ==="
	curl -s -X POST http://localhost:8080/accounts/acc-001/withdrawals \
	  -H "Content-Type: application/json" \
	  -d '{"amount":3000}' | cat

	@echo "\n=== 振り込み (acc-001 -> acc-002, 2000円) ==="
	curl -s -X POST http://localhost:8080/accounts/acc-001/transfers \
	  -H "Content-Type: application/json" \
	  -d '{"to_account_id":"acc-002","amount":2000}' | cat

	@echo "\n=== 残高照会 acc-001 ==="
	curl -s http://localhost:8080/accounts/acc-001 | cat

	@echo "\n=== 残高照会 acc-002 (振り込み入金を確認) ==="
	curl -s http://localhost:8080/accounts/acc-002 | cat

	@echo ""
