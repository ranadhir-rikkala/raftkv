package kv

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ranadhir-rikkala/raftkv/transport/memnet"
)

var bg = context.Background()

func mustGet(t *testing.T, ck *Clerk, key string) string {
	t.Helper()
	v, err := ck.Get(bg, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v
}

func TestOpCodecRoundTrip(t *testing.T) {
	ops := []Op{
		{},
		{Kind: OpAppend, Key: "k", Value: "v", ClientID: 1<<62 + 3, Seq: 42},
		{Kind: OpPut, Key: "", Value: "only-value", ClientID: 7, Seq: 1},
		{Kind: OpGet, Key: "kéy\x00", ClientID: 9, Seq: 1 << 40},
	}
	for _, op := range ops {
		got, err := decodeOp(encodeOp(op))
		if err != nil || got != op {
			t.Fatalf("round trip %+v -> %+v, %v", op, got, err)
		}
	}
	if _, err := decodeOp([]byte{1, 2}); err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestBasicPutGetAppend(t *testing.T) {
	c := newCluster(t, 3, true)
	ck := c.clerk()

	if v := mustGet(t, ck, "missing"); v != "" {
		t.Fatalf("Get(missing) = %q", v)
	}
	if err := ck.Put(bg, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := ck.Append(bg, "a", "2"); err != nil {
		t.Fatal(err)
	}
	if err := ck.Append(bg, "b", "x"); err != nil {
		t.Fatal(err)
	}
	if v := mustGet(t, ck, "a"); v != "12" {
		t.Fatalf("Get(a) = %q, want 12", v)
	}
	if v := mustGet(t, c.clerk(), "b"); v != "x" {
		t.Fatalf("Get(b) = %q, want x", v)
	}
}

// TestDuplicateRequestNotReapplied sends the exact same Append (same client
// ID and sequence number) to the leader twice, as a client would after a lost
// reply, and checks it is applied once.
func TestDuplicateRequestNotReapplied(t *testing.T) {
	c := newCluster(t, 3, true)
	ck := c.clerk()
	if err := ck.Put(bg, "k", "a"); err != nil {
		t.Fatal(err)
	}
	leader := c.waitLeader()
	ep := c.net.Endpoint(memnet.Client, leader)
	args := PutAppendArgs{Kind: OpAppend, Key: "k", Value: "b", ClientID: 12345, Seq: 1}
	for range 3 {
		var reply PutAppendReply
		if err := ep.Call(bg, "KV.PutAppend", &args, &reply); err != nil || reply.Err != OK {
			t.Fatalf("append: err=%v reply=%v", err, reply.Err)
		}
	}
	if v := mustGet(t, ck, "k"); v != "ab" {
		t.Fatalf("Get(k) = %q, want ab", v)
	}
}

// checkAppends verifies that every client's appended tokens appear exactly
// once and in the order that client issued them.
func checkAppends(t *testing.T, v string, clients, perClient int) {
	t.Helper()
	for c := range clients {
		last := -1
		for i := range perClient {
			tok := fmt.Sprintf("[%d.%d]", c, i)
			if n := strings.Count(v, tok); n != 1 {
				t.Fatalf("token %s appears %d times", tok, n)
			}
			pos := strings.Index(v, tok)
			if pos < last {
				t.Fatalf("token %s out of order", tok)
			}
			last = pos
		}
	}
}

func runAppenders(t *testing.T, c *cluster, clients, perClient int) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for id := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ck := c.clerk()
			for i := range perClient {
				if err := ck.Append(bg, "log", fmt.Sprintf("[%d.%d]", id, i)); err != nil {
					errs <- err
					return
				}
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestConcurrentAppends(t *testing.T) {
	const clients, perClient = 5, 40
	c := newCluster(t, 3, true)
	runAppenders(t, c, clients, perClient)
	checkAppends(t, mustGet(t, c.clerk(), "log"), clients, perClient)
}

// TestUnreliableRetriesDoNotDuplicate runs concurrent appenders over a
// network that drops 10% of requests and 10% of replies and delays the rest.
// Every dropped reply forces a client retry of an op that already committed;
// the dedup table must absorb all of them.
func TestUnreliableRetriesDoNotDuplicate(t *testing.T) {
	const clients, perClient = 5, 30
	c := newCluster(t, 5, false)
	runAppenders(t, c, clients, perClient)
	c.net.SetReliable(true)
	checkAppends(t, mustGet(t, c.clerk(), "log"), clients, perClient)
}

func TestMinorityPartitionMakesNoProgress(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)
	ck := c.clerk()
	if err := ck.Put(bg, "k", "0"); err != nil {
		t.Fatal(err)
	}

	leader := c.waitLeader()
	minority := []int{leader, (leader + 1) % n}
	majority := []int{(leader + 2) % n, (leader + 3) % n, (leader + 4) % n}
	c.net.Partition(majority, minority)

	ctx, cancel := context.WithTimeout(bg, 2*time.Second)
	err := c.clerkFor(minority...).Put(ctx, "k", "minority")
	cancel()
	if err == nil {
		t.Fatal("minority partition committed a write")
	}

	majorityCk := c.clerkFor(majority...)
	if err := majorityCk.Append(bg, "k", "1"); err != nil {
		t.Fatal(err)
	}
	if v := mustGet(t, majorityCk, "k"); v != "01" {
		t.Fatalf("majority Get(k) = %q, want 01", v)
	}

	c.net.Heal()
	v := mustGet(t, c.clerk(), "k")
	if v != "01" && v != "minority" {
		t.Fatalf("after heal Get(k) = %q", v)
	}
}

func TestRestartReplaysLog(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	ck := c.clerk()
	for i := range 20 {
		if err := ck.Append(bg, "k", fmt.Sprint(i%10)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		c.crash(i)
	}
	for i := range n {
		c.start(i)
	}
	if v := mustGet(t, c.clerk(), "k"); v != "01234567890123456789" {
		t.Fatalf("after restart Get(k) = %q", v)
	}
	if err := ck.Append(bg, "k", "x"); err != nil {
		t.Fatal(err)
	}
	if v := mustGet(t, ck, "k"); !strings.HasSuffix(v, "9x") || len(v) != 21 {
		t.Fatalf("Get(k) = %q", v)
	}
}
