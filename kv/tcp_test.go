package kv

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ranadhir-rikkala/raftkv/raft"
	"github.com/ranadhir-rikkala/raftkv/transport"
	"github.com/ranadhir-rikkala/raftkv/transport/rpcnet"
)

// TestClusterOverTCP runs a 3-node cluster over real sockets with net/rpc
// and file-backed persistence, kills the leader, and checks that writes keep
// working and that acknowledged data survives.
func TestClusterOverTCP(t *testing.T) {
	const n = 3
	lns := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i], addrs[i] = ln, ln.Addr().String()
	}
	endpoints := func() []transport.Endpoint {
		ends := make([]transport.Endpoint, n)
		for i, a := range addrs {
			ends[i] = rpcnet.NewEndpoint(a)
		}
		return ends
	}

	dir := t.TempDir()
	servers := make([]*Server, n)
	for i := range n {
		p, err := raft.NewFilePersister(filepath.Join(dir, fmt.Sprintf("node%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		s, err := NewServer(endpoints(), i, p, raft.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		rs := rpcnet.NewServer()
		if err := rs.Register("Raft", s.Raft()); err != nil {
			t.Fatal(err)
		}
		if err := rs.Register("KV", s); err != nil {
			t.Fatal(err)
		}
		go rs.Serve(lns[i])
		servers[i] = s
	}
	t.Cleanup(func() {
		for i := range n {
			lns[i].Close()
			servers[i].Kill()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ck := NewClerk(endpoints())
	if err := ck.Put(ctx, "k", "a"); err != nil {
		t.Fatal(err)
	}

	leader := -1
	for i, s := range servers {
		if _, ok := s.Raft().State(); ok {
			leader = i
		}
	}
	if leader == -1 {
		t.Fatal("no leader after a successful write")
	}
	lns[leader].Close()
	servers[leader].Kill()

	if err := ck.Append(ctx, "k", "b"); err != nil {
		t.Fatal(err)
	}
	v, err := ck.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if v != "ab" {
		t.Fatalf("Get(k) = %q, want ab", v)
	}
}
