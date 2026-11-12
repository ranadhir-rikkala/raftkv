#!/usr/bin/env bash
set -euo pipefail

DIR=.cluster
PEERS=${PEERS:-127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003}
IFS=, read -r -a ADDRS <<<"$PEERS"

start() {
  mkdir -p "$DIR"
  for i in "${!ADDRS[@]}"; do
    pidfile="$DIR/node$i.pid"
    if [[ -f $pidfile ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
      echo "node$i already running (pid $(cat "$pidfile"))"
      continue
    fi
    ./bin/raftkv-server -id "$i" -peers "$PEERS" -data "$DIR/data" >"$DIR/node$i.log" 2>&1 &
    echo $! >"$pidfile"
    echo "node$i started on ${ADDRS[$i]} (pid $!, log $DIR/node$i.log)"
  done
  echo
  echo "try: ./bin/raftkv-cli status"
  echo "     ./bin/raftkv-cli put greeting hello"
  echo "     ./bin/raftkv-cli get greeting"
}

stop() {
  for pidfile in "$DIR"/node*.pid; do
    [[ -e $pidfile ]] || continue
    pid=$(cat "$pidfile")
    if kill "$pid" 2>/dev/null; then
      echo "stopped $(basename "$pidfile" .pid) (pid $pid)"
    fi
    rm -f "$pidfile"
  done
}

kill_node() {
  pidfile="$DIR/node$1.pid"
  kill -9 "$(cat "$pidfile")"
  rm -f "$pidfile"
  echo "killed node$1"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  kill) kill_node "${2:?node id}" ;;
  *) echo "usage: $0 start|stop|kill <id>" >&2; exit 2 ;;
esac
