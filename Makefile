GO     ?= go
BIN    := bin
PEERS  := 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003

.PHONY: build test test-short vet stress cluster cluster-stop cluster-clean docker-up docker-down

build:
	$(GO) build -o $(BIN)/ ./cmd/...

vet:
	$(GO) vet ./...

test:
	$(GO) test -race ./...

test-short:
	$(GO) test -race -short ./...

stress:
	$(GO) test -race -count=5 -run 'Election|Partition|Rejoin|Backup|Figure8' ./raft/
	$(GO) test -race -count=5 -run 'Linearizable|Duplicate|Unreliable' ./kv/

cluster: build
	PEERS=$(PEERS) ./scripts/cluster.sh start

cluster-stop:
	./scripts/cluster.sh stop

cluster-clean: cluster-stop
	rm -rf .cluster

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
