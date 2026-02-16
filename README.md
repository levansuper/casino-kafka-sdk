# casino-kafka-sdk

A type-safe Kafka SDK for the Casino platform, built on top of [KafkaJS](https://kafka.js.org/). Provides strongly-typed producers and consumers with predefined event types and data schemas for financial, user, and server events.

## Features

- **Full TypeScript generics** — compile-time verification that message payloads match their topic
- **Predefined event catalog** — 12 event types across 3 categories (financial, user, server)
- **Pluggable serialization** — JSON by default, swap in your own `Serializer` implementation
- **Batch sending** — send multiple messages to a topic in a single call
- **Concurrency control** — sequential or parallel partition processing for consumers
- **Configurable error handling** — propagate errors to KafkaJS for retry, or log and swallow them
- **Dual module output** — works with both ESM (`import`) and CommonJS (`require`)

## Installation

```bash
npm install casino-kafka-sdk
```

## Quick Start

```ts
import { KafkaClient, UserEvent, FinancialEvent } from 'casino-kafka-sdk';

const client = new KafkaClient({
  brokers: ['localhost:9092'],
  clientId: 'casino-example',
});

// --- Producer ---
const producer = client.createProducer();
await producer.connect();

await producer.send(UserEvent.Login, {
  key: 'user-1',
  value: { userId: 'user-1', ip: '192.168.1.1' },
});

await producer.sendBatch(FinancialEvent.Transaction, [
  { key: 'user-2', value: { userId: 'user-2', amount: 50, currency: 'USD', transactionId: 'tx-001' } },
  { key: 'user-3', value: { userId: 'user-3', amount: 200, currency: 'USD', transactionId: 'tx-002' } },
]);

await producer.disconnect();

// --- Consumer ---
const consumer = client.createConsumer({ groupId: 'casino-example-group' });
await consumer.connect();

await consumer.subscribe(
  FinancialEvent.Transaction,
  async (message) => {
    console.log(`Received [${message.key}]:`, message.value);
  },
  { fromBeginning: true },
);
```

## API Reference

### `KafkaClient`

Main entry point. Wraps a KafkaJS `Kafka` instance.

```ts
const client = new KafkaClient({
  brokers: ['localhost:9092'],
  clientId: 'my-service',
  logLevel: logLevel.INFO,       // optional — KafkaJS log level
  kafkaOptions: { /* ... */ },   // optional — pass-through KafkaJS config
});
```

| Method | Returns | Description |
|---|---|---|
| `createProducer(config?)` | `SdkProducer` | Create a new producer instance |
| `createConsumer(config)` | `SdkConsumer` | Create a new consumer instance |

### `SdkProducer`

Type-safe message producer.

```ts
const producer = client.createProducer({
  serializer: new JsonSerializer(), // optional — default is JsonSerializer
});
```

| Method | Description |
|---|---|
| `connect()` | Connect to the Kafka broker |
| `disconnect()` | Disconnect from the Kafka broker |
| `send(topic, message)` | Send a single message. The `value` type is inferred from the topic |
| `sendBatch(topic, messages)` | Send multiple messages to the same topic in one call |

**Message shape:**

```ts
{
  key?: string;          // optional partition key
  value: TopicDataMap[T]; // payload — type-checked against the topic
  headers?: Record<string, string>; // optional headers
}
```

### `SdkConsumer`

Type-safe message consumer.

```ts
const consumer = client.createConsumer({
  groupId: 'my-group',
  serializer: new JsonSerializer(), // optional
  sequential: true,                 // optional — default true, process messages one at a time
  concurrency: 4,                   // optional — ignored when sequential is true
  propagateErrors: true,            // optional — default true
});
```

| Method | Description |
|---|---|
| `connect()` | Connect to the Kafka broker |
| `disconnect()` | Disconnect from the Kafka broker |
| `subscribe(topic, handler, options?)` | Subscribe to a topic and start consuming |

**Handler signature:**

```ts
async (message: ConsumedMessage<T>) => void
```

**ConsumedMessage fields:**

| Field | Type | Description |
|---|---|---|
| `topic` | `T` | The topic name |
| `partition` | `number` | Partition number |
| `offset` | `string` | Message offset |
| `key` | `string \| null` | Message key |
| `value` | `TopicDataMap[T]` | Deserialized payload |
| `headers` | `Record<string, string \| undefined>` | Message headers |
| `timestamp` | `string` | Message timestamp |

**Error handling modes:**

- `propagateErrors: true` (default) — handler errors bubble up to KafkaJS, preventing offset commit so the message is retried
- `propagateErrors: false` — errors are logged to stderr and the offset is committed

### `JsonSerializer`

Default serializer. Converts data to/from JSON via `Buffer`.

```ts
import { JsonSerializer } from 'casino-kafka-sdk';

const serializer = new JsonSerializer();
const buf = serializer.serialize({ foo: 'bar' }); // Buffer
const obj = serializer.deserialize(buf);           // { foo: 'bar' }
```

Implement the `Serializer` interface to provide your own:

```ts
interface Serializer<T = unknown> {
  serialize(data: T): Buffer;
  deserialize(buffer: Buffer): T;
}
```

## Event Catalog

### Financial Events (`FinancialEvent`)

| Event | Topic String | Payload |
|---|---|---|
| `Transaction` | `financial-event.transaction` | `{ userId, amount, currency, transactionId }` |
| `Win` | `financial-event.win` | `{ userId, amount, gameId }` |
| `Loss` | `financial-event.loss` | `{ userId, amount, gameId }` |
| `Deposit` | `financial-event.deposit` | `{ userId, amount, currency, method }` |
| `Withdrawal` | `financial-event.withdrawal` | `{ userId, amount, currency, method }` |

### User Events (`UserEvent`)

| Event | Topic String | Payload |
|---|---|---|
| `Login` | `user-event.login` | `{ userId, ip }` |
| `Logout` | `user-event.logout` | `{ userId }` |
| `Register` | `user-event.register` | `{ userId, email }` |
| `SessionExpired` | `user-event.session-expired` | `{ userId, sessionId }` |

### Server Events (`ServerEvent`)

| Event | Topic String | Payload |
|---|---|---|
| `Crash` | `server-event.crash` | `{ serverId, error }` |
| `HealthCheck` | `server-event.health-check` | `{ serverId, status }` |
| `Restart` | `server-event.restart` | `{ serverId, reason }` |

## Local Kafka Setup

A Docker Compose file is included for local development:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

This starts a single-node Kafka 3.7.0 broker in KRaft mode on `localhost:9092`.

## Development

```bash
# Install dependencies
npm install

# Build (CJS + ESM + type declarations)
npm run build

# Watch mode
npm run dev

# Run tests
npm run test
```

## License

MIT
