package memnet

import (
	"context"
	"errors"
	"testing"

	"github.com/ranadhir-rikkala/raftkv/transport"
)

type EchoArgs struct{ Msg string }
type EchoReply struct{ Msg string }

type echo struct{ calls int }

func (e *echo) Echo(args *EchoArgs, reply *EchoReply) error {
	e.calls++
	reply.Msg = args.Msg
	return nil
}

func newEchoNet(t *testing.T, n int) (*Network, []*echo) {
	t.Helper()
	net := New(1)
	t.Cleanup(net.Close)
	svcs := make([]*echo, n)
	for i := range n {
		srv := NewServer()
		svcs[i] = &echo{}
		if err := srv.Register("Echo", svcs[i]); err != nil {
			t.Fatal(err)
		}
		net.AddServer(i, srv)
	}
	return net, svcs
}

func call(e transport.Endpoint) error {
	var reply EchoReply
	if err := e.Call(context.Background(), "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply); err != nil {
		return err
	}
	if reply.Msg != "hi" {
		return errors.New("bad reply")
	}
	return nil
}

func TestPartitionAndHeal(t *testing.T) {
	net, _ := newEchoNet(t, 3)
	if err := call(net.Endpoint(0, 1)); err != nil {
		t.Fatalf("connected call failed: %v", err)
	}
	net.Partition([]int{0}, []int{1, 2})
	if err := call(net.Endpoint(0, 1)); !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("cross-partition call: got %v", err)
	}
	if err := call(net.Endpoint(1, 2)); err != nil {
		t.Fatalf("same-partition call failed: %v", err)
	}
	if err := call(net.Endpoint(Client, 0)); err != nil {
		t.Fatalf("client call failed: %v", err)
	}
	net.Heal()
	if err := call(net.Endpoint(0, 1)); err != nil {
		t.Fatalf("call after heal failed: %v", err)
	}
}

func TestRestartFencesOldEndpoints(t *testing.T) {
	net, _ := newEchoNet(t, 2)
	old := net.Endpoint(0, 1)
	net.AddServer(0, NewServer())
	if err := call(old); !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("endpoint from previous incarnation: got %v", err)
	}
	if err := call(net.Endpoint(0, 1)); err != nil {
		t.Fatalf("endpoint from new incarnation failed: %v", err)
	}
}

func TestUnreliableDropsSomeCalls(t *testing.T) {
	net, svcs := newEchoNet(t, 2)
	net.SetReliable(false)
	const total = 200
	failed := 0
	for range total {
		if call(net.Endpoint(0, 1)) != nil {
			failed++
		}
	}
	if failed == 0 || failed == total {
		t.Fatalf("%d of %d calls failed, want some but not all", failed, total)
	}
	if svcs[1].calls <= total-failed {
		t.Fatalf("handler ran %d times for %d successes; expected some lost replies", svcs[1].calls, total-failed)
	}
}
