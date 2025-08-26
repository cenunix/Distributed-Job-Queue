.PHONY: up down reset logs bench test test-only

up:
	docker compose up --build -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f worker

bench:
	python scripts/bench.py --jobs 500 --concurrency 50 --seconds-per-job 0.05 --priority high

# Run tests against a fresh stack
test:
	docker compose up --build -d
	sleep 5
	pytest -q tests/test_basic.py
	docker compose down

# If your stack is already up, just run tests
test-only:
	pytest -q tests/test_basic.py

