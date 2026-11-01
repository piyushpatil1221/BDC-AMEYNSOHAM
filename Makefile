# Run with `make <target>` (Linux/macOS/WSL) or `mingw32-make <target>` on Windows.
# Recipes use only `docker compose` so they behave the same under sh and cmd.

COMPOSE = docker compose
TOOLS   = $(COMPOSE) --profile core --profile tools run --rm --no-deps

.PHONY: up down data replay verify stats logs

up:            ## start the core stack and bootstrap topics/collections/schema
	$(COMPOSE) --profile core up -d --build

down:          ## stop everything (volumes kept; add -v manually to wipe state)
	$(COMPOSE) --profile core --profile tools --profile submit down

data:          ## download MIND-small and build the replay subset
	$(TOOLS) replay replay.download
	$(TOOLS) replay replay.subset

replay:        ## replay the subset into Kafka (override: make replay ARGS="--speedup 120")
	$(TOOLS) replay replay.producer $(ARGS)

verify:        ## check articles precede their first events on the raw topics
	$(TOOLS) replay replay.verify_order $(ARGS)

stats:         ## one-shot memory snapshot of all containers
	docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}"

logs:
	$(COMPOSE) --profile core logs -f --tail 50
