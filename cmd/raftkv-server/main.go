// Command raftkv-server runs one node of a raftkv cluster.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ranadhir-rikkala/raftkv/kv"
	"github.com/ranadhir-rikkala/raftkv/raft"
	"github.com/ranadhir-rikkala/raftkv/transport"
	"github.com/ranadhir-rikkala/raftkv/transport/rpcnet"
)

func main() {
	id := flag.Int("id", 0, "index of this node in -peers")
	peers := flag.String("peers", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated addresses of all nodes, in id order")
	listen := flag.String("listen", "", "address to listen on (default: this node's entry in -peers)")
	dataDir := flag.String("data", "data", "directory for durable Raft state")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", *id)
	if err := run(logger, *id, strings.Split(*peers, ","), *listen, *dataDir); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, id int, addrs []string, listen, dataDir string) error {
	if id < 0 || id >= len(addrs) {
		return fmt.Errorf("id %d out of range for %d peers", id, len(addrs))
	}
	if listen == "" {
		listen = addrs[id]
	}

	persister, err := raft.NewFilePersister(filepath.Join(dataDir, fmt.Sprintf("node%d", id)))
	if err != nil {
		return err
	}
	ends := make([]transport.Endpoint, len(addrs))
	for i, a := range addrs {
		ends[i] = rpcnet.NewEndpoint(a)
	}

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	server, err := kv.NewServer(ends, id, persister, raft.DefaultConfig())
	if err != nil {
		return err
	}
	defer server.Kill()

	rpcs := rpcnet.NewServer()
	if err := rpcs.Register("Raft", server.Raft()); err != nil {
		return err
	}
	if err := rpcs.Register("KV", server); err != nil {
		return err
	}
	go rpcs.Serve(ln)
	logger.Info("listening", "addr", ln.Addr().String(), "peers", addrs, "data", dataDir)

	go watchRole(logger, server.Raft())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	logger.Info("shutting down", "signal", s.String())
	return nil
}

func watchRole(logger *slog.Logger, rf *raft.Raft) {
	lastTerm, lastRole := -1, raft.Role(-1)
	for range time.Tick(100 * time.Millisecond) {
		term, _ := rf.State()
		role := rf.Role()
		if term != lastTerm || role != lastRole {
			logger.Info("state", "term", term, "role", role.String())
			lastTerm, lastRole = term, role
		}
	}
}
