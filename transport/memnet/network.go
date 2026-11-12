// Package memnet is an in-process network for tests. It can disconnect
// nodes, split them into partitions, drop and delay requests and replies, and
// hold replies back long enough that they arrive out of order.
package memnet

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ranadhir-rikkala/raftkv/transport"
)

// Client is the "from" id for endpoints held by external clients. Client
// endpoints are never subject to partitions, only to the target being
// disconnected and to unreliable delivery.
const Client = -1

type node struct {
	srv       *Server
	gen       int
	connected bool
	group     int
}

type Network struct {
	mu             sync.Mutex
	nodes          map[int]*node
	reliable       bool
	longDelays     bool
	longReordering bool
	rng            *rand.Rand
	rngMu          sync.Mutex
	rpcs           atomic.Int64
	done           chan struct{}
	closeOnce      sync.Once
}

func New(seed int64) *Network {
	return &Network{
		nodes:    make(map[int]*node),
		reliable: true,
		rng:      rand.New(rand.NewSource(seed)),
		done:     make(chan struct{}),
	}
}

// AddServer installs srv as node id, replacing any previous incarnation.
// Endpoints created by or pointing at an older incarnation stop working, so a
// crashed node's in-flight traffic cannot leak into the new one.
func (n *Network) AddServer(id int, srv *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	nd, ok := n.nodes[id]
	if !ok {
		nd = &node{connected: true}
		n.nodes[id] = nd
	}
	nd.srv = srv
	nd.gen++
}

// Crash invalidates node id's current incarnation without installing a new
// one.
func (n *Network) Crash(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if nd, ok := n.nodes[id]; ok {
		nd.srv = nil
		nd.gen++
	}
}

func (n *Network) Connect(id int)    { n.setConnected(id, true) }
func (n *Network) Disconnect(id int) { n.setConnected(id, false) }

func (n *Network) setConnected(id int, v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if nd, ok := n.nodes[id]; ok {
		nd.connected = v
	}
}

func (n *Network) IsConnected(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	nd, ok := n.nodes[id]
	return ok && nd.connected
}

// Partition splits nodes into groups that can only talk within themselves.
// Nodes not listed in any group end up in a group of their own.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	next := len(groups) + 1
	for id, nd := range n.nodes {
		nd.group = next + id
	}
	for g, members := range groups {
		for _, id := range members {
			if nd, ok := n.nodes[id]; ok {
				nd.group = g + 1
			}
		}
	}
}

func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, nd := range n.nodes {
		nd.group = 0
		nd.connected = true
	}
}

func (n *Network) SetReliable(v bool) {
	n.mu.Lock()
	n.reliable = v
	n.mu.Unlock()
}

func (n *Network) SetLongDelays(v bool) {
	n.mu.Lock()
	n.longDelays = v
	n.mu.Unlock()
}

func (n *Network) SetLongReordering(v bool) {
	n.mu.Lock()
	n.longReordering = v
	n.mu.Unlock()
}

func (n *Network) RPCCount() int64 { return n.rpcs.Load() }

func (n *Network) Close() { n.closeOnce.Do(func() { close(n.done) }) }

func (n *Network) Endpoint(from, to int) transport.Endpoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	e := &endpoint{net: n, from: from, to: to}
	if nd, ok := n.nodes[from]; ok {
		e.fromGen = nd.gen
	}
	return e
}

func (n *Network) intn(k int) int {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	return n.rng.Intn(k)
}

type route struct {
	srv    *Server
	toGen  int
	ok     bool
	unrel  bool
	delays bool
	reord  bool
}

func (n *Network) route(e *endpoint) route {
	n.mu.Lock()
	defer n.mu.Unlock()
	r := route{unrel: !n.reliable, delays: n.longDelays, reord: n.longReordering}
	to, ok := n.nodes[e.to]
	if !ok || to.srv == nil || !to.connected {
		return r
	}
	if e.from != Client {
		from, ok := n.nodes[e.from]
		if !ok || from.gen != e.fromGen || !from.connected || from.group != to.group {
			return r
		}
	}
	r.srv, r.toGen, r.ok = to.srv, to.gen, true
	return r
}

func (n *Network) stillRoutable(e *endpoint, toGen int) bool {
	r := n.route(e)
	return r.ok && r.toGen == toGen
}

type endpoint struct {
	net     *Network
	from    int
	fromGen int
	to      int
}

type result struct {
	data []byte
	err  error
}

func (e *endpoint) Call(ctx context.Context, method string, args, reply any) error {
	n := e.net
	n.rpcs.Add(1)

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		return err
	}

	r := n.route(e)
	if r.unrel {
		if err := n.sleep(ctx, time.Duration(n.intn(27))*time.Millisecond); err != nil {
			return err
		}
		if n.intn(1000) < 100 {
			return transport.ErrDropped
		}
	}
	if !r.ok {
		max := 100
		if r.delays {
			max = 7000
		}
		if err := n.sleep(ctx, time.Duration(n.intn(max))*time.Millisecond); err != nil {
			return err
		}
		return transport.ErrUnreachable
	}

	ch := make(chan result, 1)
	go func() {
		data, err := r.srv.dispatch(method, buf.Bytes())
		ch <- result{data, err}
	}()

	var res result
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
wait:
	for {
		select {
		case res = <-ch:
			break wait
		case <-ticker.C:
			if !n.stillRoutable(e, r.toGen) {
				return transport.ErrUnreachable
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return transport.ErrUnreachable
		}
	}

	if !n.stillRoutable(e, r.toGen) {
		return transport.ErrUnreachable
	}
	if res.err != nil {
		return res.err
	}
	if r.unrel && n.intn(1000) < 100 {
		return transport.ErrDropped
	}
	if r.reord && n.intn(900) < 600 {
		d := 200 + n.intn(1+n.intn(2000))
		if err := n.sleep(ctx, time.Duration(d)*time.Millisecond); err != nil {
			return err
		}
	}
	return gob.NewDecoder(bytes.NewReader(res.data)).Decode(reply)
}

func (n *Network) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return transport.ErrUnreachable
	}
}
