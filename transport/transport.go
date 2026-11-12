// Package transport defines how raftkv nodes and clients talk to each other.
//
// Both Raft peers and KV clients hold one Endpoint per remote node. Handlers
// on the receiving side follow the net/rpc convention:
//
//	func (t *T) Method(args *Args, reply *Reply) error
//
// so the same receiver can be served by the in-memory test network
// (transport/memnet) or by a real TCP server (transport/rpcnet).
package transport

import (
	"context"
	"errors"
)

// Endpoint is a client-side handle to a single remote node.
type Endpoint interface {
	// Call invokes method (for example "Raft.AppendEntries") on the remote
	// node. A nil error means reply has been filled in by the remote handler.
	// Any non-nil error means the outcome is unknown: the request may or may
	// not have been executed.
	Call(ctx context.Context, method string, args, reply any) error
}

var (
	ErrUnreachable = errors.New("transport: peer unreachable")
	ErrDropped     = errors.New("transport: message dropped")
)
