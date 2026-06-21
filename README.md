# TCP Broker

A TCP-based pub/sub message broker with at-least-once delivery guarantees, replay-on-reconnect, and retry-on-timeout. Written in Go as a learning project.

## Architecture

```
producer --TCP--> broker --TCP--> consumer(s)
```

Three components communicating over raw TCP on port `:8090`:

- **Broker** — central hub, manages subscriptions, routes messages, tracks delivery
- **Producer** — publishes messages to a topic
- **Consumer** — subscribes to a topic and receives messages

## Protocol

Plain text over TCP, `\n`-delimited messages:

| Command | Format | Description |
|---------|--------|-------------|
| SUB | `SUB <topic> <consumerID>\n` | Subscribe to a topic |
| UNSUB | `UNSUB <topic>\n` | Unsubscribe from a topic |
| PUB | `PUB <topic> <message>\n` | Publish a message |
| LOG OK | `LOG OK <consumerID>\n` | Acknowledge message delivery |
| LOG KO | `LOG KO <consumerID>\n` | Report delivery failure |

## Usage

```bash
# Start the broker
go run ./broker/

# Subscribe (consumer)
go run ./consumer/ <topic>

# Publish (producer)
go run ./producer/ <topic> <message>
```

## Features

- **Pub/sub** — topics with multiple subscribers
- **At-least-once delivery** — messages tracked via `inFlight` map, retried on timeout
- **Replay-on-reconnect** — undelivered messages stored in `msgBackup`, replayed on reconnect
- **Graceful shutdown** — signal handler (SIGINT/SIGTERM) closes all connections cleanly
- **Per-consumer delivery goroutine** — one goroutine per consumer with dedicated channels, no shared state races
