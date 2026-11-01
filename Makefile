# Run with `make <target>` (Linux/macOS/WSL) or `mingw32-make <target>` on Windows.
# Recipes use only `docker compose` so they behave the same under sh and cmd.

COMPOSE = docker compose
TOOLS   = $(COMPOSE) --profile core --profile tools run --rm --no-deps
SUBMIT  = $(COMPOSE) --profile core --profile submit run --rm --no-deps flink-submit

.PHONY: up up-all down reset submit data replay burst loadgen verify counts test smoke eval ablation stats logs

up:            ## start the core stack, bootstrap topics/collections/schema, submit the Flink job
	$(COMPOSE) --profile core up -d --build
	$(SUBMIT)

up-all:        ## core + Prometheus/Grafana
	$(COMPOSE) --profile core --profile monitoring up -d --build
	$(SUBMIT)

down:          ## stop everything (volumes kept)
	$(COMPOSE) --profile core --profile monitoring --profile tools --profile submit down

reset:         ## stop everything and wipe all state (Kafka, Qdrant, ScyllaDB, checkpoints)
	$(COMPOSE) --profile core --profile monitoring --profile tools --profile submit down -v

submit:        ## (re)submit the Flink job, e.g. make submit ARGS="--eviction off"
	$(SUBMIT) $(ARGS)

data:          ## download MIND-small and build the replay subset
	$(TOOLS) replay replay.download
	$(TOOLS) replay replay.subset

replay:        ## replay the subset into Kafka (override: make replay ARGS="--speedup 120")
	$(TOOLS) replay replay.producer $(ARGS)

burst:         ## inject a breaking-news burst (override: make burst ARGS="--category finance")
	$(TOOLS) replay replay.burst $(ARGS)

loadgen:       ## steady /recommend traffic for latency metrics
	$(TOOLS) replay replay.loadgen $(ARGS)

verify:        ## check articles precede their first events on the raw topics
	$(TOOLS) replay replay.verify_order $(ARGS)

counts:        ## records per topic
	$(TOOLS) replay replay.topic_counts

test:          ## unit tests
	$(TOOLS) tests tests/unit

smoke:         ## end-to-end smoke test against the running stack
	$(TOOLS) tests tests/integration

eval:          ## offline quality: AUC / MRR / nDCG vs popularity baseline -> reports/eval.md
	$(TOOLS) eval

ablation:      ## TTL/eviction ON vs OFF -> reports/ablation_*.png (wipes state between runs)
	python eval/ablation.py

stats:         ## one-shot memory snapshot of all containers
	docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}"

logs:
	$(COMPOSE) --profile core logs -f --tail 50
