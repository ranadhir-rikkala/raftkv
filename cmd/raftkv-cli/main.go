// Command raftkv-cli talks to a raftkv cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ranadhir-rikkala/raftkv/kv"
	"github.com/ranadhir-rikkala/raftkv/transport"
	"github.com/ranadhir-rikkala/raftkv/transport/rpcnet"
)

const usage = `usage: raftkv-cli [-servers a,b,c] [-timeout d] <command>

commands:
  get <key>
  put <key> <value>
  append <key> <value>
  status
`

func main() {
	servers := flag.String("servers", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated server addresses")
	timeout := flag.Duration("timeout", 5*time.Second, "give up after this long")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage); flag.PrintDefaults() }
	flag.Parse()

	addrs := strings.Split(*servers, ",")
	ends := make([]transport.Endpoint, len(addrs))
	for i, a := range addrs {
		ends[i] = rpcnet.NewEndpoint(a)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := run(ctx, kv.NewClerk(ends), addrs, ends, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, ck *kv.Clerk, addrs []string, ends []transport.Endpoint, args []string) error {
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	switch cmd, rest := args[0], args[1:]; {
	case cmd == "get" && len(rest) == 1:
		v, err := ck.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		fmt.Println(v)
	case cmd == "put" && len(rest) == 2:
		if err := ck.Put(ctx, rest[0], rest[1]); err != nil {
			return err
		}
		fmt.Println("OK")
	case cmd == "append" && len(rest) == 2:
		if err := ck.Append(ctx, rest[0], rest[1]); err != nil {
			return err
		}
		fmt.Println("OK")
	case cmd == "status" && len(rest) == 0:
		for i, ep := range ends {
			var reply kv.StatusReply
			cctx, cancel := context.WithTimeout(ctx, time.Second)
			err := ep.Call(cctx, "KV.Status", &kv.StatusArgs{}, &reply)
			cancel()
			if err != nil {
				fmt.Printf("node%d %-21s unreachable (%v)\n", i, addrs[i], err)
				continue
			}
			fmt.Printf("node%d %-21s term=%d role=%s applied=%d keys=%d\n", i, addrs[i], reply.Term, reply.Role, reply.LastApplied, reply.Keys)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	return nil
}
