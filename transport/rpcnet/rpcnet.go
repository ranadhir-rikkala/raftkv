// Package rpcnet carries raftkv traffic over TCP using net/rpc and gob.
package rpcnet

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Endpoint lazily dials addr and redials after connection failures.
type Endpoint struct {
	addr        string
	dialTimeout time.Duration

	mu     sync.Mutex
	client *rpc.Client
}

func NewEndpoint(addr string) *Endpoint {
	return &Endpoint{addr: addr, dialTimeout: 500 * time.Millisecond}
}

func (e *Endpoint) Call(ctx context.Context, method string, args, reply any) error {
	c, err := e.conn(ctx)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			var se rpc.ServerError
			if !errors.As(call.Error, &se) {
				e.reset(c)
			}
		}
		return call.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Endpoint) conn(ctx context.Context) (*rpc.Client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		return e.client, nil
	}
	d := net.Dialer{Timeout: e.dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", e.addr)
	if err != nil {
		return nil, err
	}
	e.client = rpc.NewClient(conn)
	return e.client, nil
}

func (e *Endpoint) reset(c *rpc.Client) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client == c {
		e.client.Close()
		e.client = nil
	}
}

func (e *Endpoint) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client == nil {
		return nil
	}
	err := e.client.Close()
	e.client = nil
	return err
}

// Server serves registered receivers on a TCP listener.
type Server struct {
	rpc *rpc.Server
}

func NewServer() *Server {
	return &Server{rpc: rpc.NewServer()}
}

func (s *Server) Register(name string, rcvr any) error {
	return s.rpc.RegisterName(name, rcvr)
}

// Serve accepts connections until ln is closed.
func (s *Server) Serve(ln net.Listener) {
	s.rpc.Accept(ln)
}
