# nestjs-sqs

[![Test](https://github.com/ssut/nestjs-sqs/workflows/Test/badge.svg)](https://github.com/ssut/nestjs-sqs/actions?query=workflow%3ATest)
[![npm version](https://badge.fury.io/js/%40ssut%2Fnestjs-sqs.svg)](https://badge.fury.io/js/%40ssut%2Fnestjs-sqs)

Tested with: [AWS SQS](https://aws.amazon.com/en/sqs/) and [ElasticMQ](https://github.com/softwaremill/elasticmq).

`nestjs-sqs` makes Amazon SQS easy to use from NestJS through **decorator-based
message handling**. It wraps [bbc/sqs-consumer](https://github.com/bbc/sqs-consumer)
and [bbc/sqs-producer](https://github.com/bbc/sqs-producer) (both on the AWS SDK
v3) and adds a NestJS-native registration, discovery, and lifecycle layer on top.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Registering the module](#registering-the-module)
  - [Async registration](#async-registration)
- [Consuming messages](#consuming-messages)
  - [Batch handling](#batch-handling)
  - [Consumer events](#consumer-events)
  - [Catch-all event handler](#catch-all-event-handler)
  - [Message acknowledgement](#message-acknowledgement)
- [Producing messages](#producing-messages)
  - [FIFO queues](#fifo-queues)
  - [Custom serializer](#custom-serializer)
- [Operational concerns](#operational-concerns)
  - [Concurrency](#concurrency)
  - [Dead-letter queues](#dead-letter-queues)
  - [Graceful shutdown](#graceful-shutdown)
  - [Custom logger](#custom-logger)
- [SqsService API](#sqsservice-api)
- [Testing](#testing)
- [License](#license)

## Requirements

- **Node.js >= 22** (the underlying `sqs-consumer` v15 / `sqs-producer` v9 track active LTS only).
- **NestJS >= 6** (tested against v11).
- **`@aws-sdk/client-sqs` >= 3.1036** as a peer dependency (AWS SDK v3).

## Installation

```shell
npm i --save @ssut/nestjs-sqs @aws-sdk/client-sqs
```

## Quick start

```ts
import { Module } from "@nestjs/common";
import { SqsModule } from "@ssut/nestjs-sqs";

@Module({
  imports: [
    SqsModule.register({
      consumers: [
        { name: "orders", queueUrl: "https://sqs.us-east-1.amazonaws.com/000/orders", region: "us-east-1" },
      ],
      producers: [
        { name: "orders", queueUrl: "https://sqs.us-east-1.amazonaws.com/000/orders", region: "us-east-1" },
      ],
    }),
  ],
})
export class AppModule {}
```

```ts
import { Injectable } from "@nestjs/common";
import { Message } from "@aws-sdk/client-sqs";
import { SqsMessageHandler, SqsService } from "@ssut/nestjs-sqs";

@Injectable()
export class OrdersConsumer {
  @SqsMessageHandler({ name: "orders" })
  public async handle(message: Message) {
    const order = JSON.parse(message.Body ?? "{}");
    // ...process the order
  }
}

@Injectable()
export class OrdersProducer {
  public constructor(private readonly sqs: SqsService) {}

  public async enqueue(order: { id: string }) {
    await this.sqs.send("orders", { id: order.id, body: order });
  }
}
```

> `name` is a unique identifier for a consumer/producer **instance** inside this
> module — it is how handlers and `send()` calls find their queue. It is not the
> SQS queue name; the queue is identified by `queueUrl`.

## Registering the module

`SqsModule.register(options)` takes:

| Option              | Type                       | Description                                                        |
| ------------------- | -------------------------- | ----------------------------------------------------------------- |
| `consumers`         | `SqsConsumerOptions[]`     | Each is a `sqs-consumer` config plus `name` and optional `stopOptions`. |
| `producers`         | `SqsProducerOptions[]`     | Each is a `sqs-producer` config plus `name`.                      |
| `logger`            | `LoggerService`            | Custom logger; defaults to a Nest `Logger`.                       |
| `globalStopOptions` | `StopOptions`              | Default stop options applied to every consumer on shutdown.       |
| `serializer`        | `(body: unknown) => string`| Encodes non-string message bodies before sending.                |

A consumer/producer accepts all of the underlying library's options (e.g.
`region`, `sqs`, `batchSize`, `waitTimeSeconds`, `pollingWaitTimeMs`,
`terminateVisibilityTimeout`, `messageAttributeNames`, `alwaysAcknowledge`). You
can also pass a pre-built `SQSClient` via `sqs` instead of `region`.

### Async registration

Use `registerAsync()` to build options from other providers (e.g. `ConfigService`):

```ts
SqsModule.registerAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    consumers: [{ name: "orders", queueUrl: config.getOrThrow("ORDERS_QUEUE_URL"), region: "us-east-1" }],
    producers: [{ name: "orders", queueUrl: config.getOrThrow("ORDERS_QUEUE_URL"), region: "us-east-1" }],
  }),
});
```

`registerAsync()` also supports `useClass` / `useExisting` (implementing
`SqsModuleOptionsFactory`), and a `providers` array for extra providers that your
factory needs to inject.

## Consuming messages

Decorate a method on any NestJS provider with `@SqsMessageHandler`. It receives
the raw AWS `Message` (parse `message.Body` yourself, or use a
[serializer](#custom-serializer) on the producing side).

```ts
@SqsMessageHandler({ name: "orders" })
public async handle(message: Message) {}
```

> The legacy positional form `@SqsMessageHandler("orders", false)` still works.

### Batch handling

Set `batch: true` to receive an array of messages via `handleMessageBatch`. Tune
how many are pulled per poll with `batchSize` (1–10):

```ts
// registration
consumers: [{ name: "orders", queueUrl: "...", batchSize: 10 }];

// handler
@SqsMessageHandler({ name: "orders", batch: true })
public async handleBatch(messages: Message[]) {}
```

### Consumer events

Attach to any [`sqs-consumer` event](https://github.com/bbc/sqs-consumer#events)
(`processing_error`, `error`, `timeout_error`, `message_received`,
`message_processed`, `empty`, `stopped`, …):

```ts
@SqsConsumerEventHandler({ name: "orders", eventName: "processing_error" })
public onProcessingError(error: Error, message: Message) {
  // report errors here
}
```

> The legacy positional form `@SqsConsumerEventHandler("orders", "processing_error")` still works.

### Catch-all event handler

Use the exported `ALL_CONSUMERS` wildcard as the name to handle an event for
**every** consumer with a single method — handy for centralized error reporting:

```ts
import { ALL_CONSUMERS, SqsConsumerEventHandler } from "@ssut/nestjs-sqs";

@SqsConsumerEventHandler(ALL_CONSUMERS, "processing_error")
public onAnyProcessingError(error: Error, message: Message) {
  // fires for processing failures on any queue
}
```

### Message acknowledgement

By default a message is **acknowledged (deleted) once its handler resolves
without throwing** — a `void`/`async` handler that finishes successfully marks
the message as processed. Throwing (or rejecting) leaves the message on the queue
for redelivery.

This is implemented by defaulting `alwaysAcknowledge: true` on every consumer.
To control acknowledgement yourself — the native `sqs-consumer` contract, where
returning `undefined` does **not** delete the message — opt out per consumer and
return the message (or an object with its `MessageId`) to ack:

```ts
consumers: [{ name: "orders", queueUrl: "...", alwaysAcknowledge: false }];
```

```ts
@SqsMessageHandler({ name: "orders" })
public async handle(message: Message) {
  if (!(await this.canProcess(message))) return; // NOT acked → redelivered
  await this.process(message);
  return message; // acked → deleted
}
```

## Producing messages

Inject `SqsService` and call `send(name, message | message[])`:

```ts
await this.sqs.send("orders", {
  id: "unique-message-id",
  body: { orderId: 42 },       // objects are JSON-serialized automatically
  delaySeconds: 0,
  messageAttributes: { source: { DataType: "String", StringValue: "api" } },
});
```

`send` accepts a single message or an array. Message shape:

| Field              | Required | Notes                                             |
| ------------------ | -------- | ------------------------------------------------- |
| `id`               | ✅       | Unique per batch; used by SQS to correlate results. |
| `body`             | ✅       | `string` sent as-is; anything else is serialized. |
| `groupId`          | FIFO     | Message group id.                                 |
| `deduplicationId`  | FIFO     | Dedup id (unless content-based dedup is enabled). |
| `delaySeconds`     | —        | Standard queues only.                             |
| `messageAttributes`| —        | SQS message attributes.                           |

### FIFO queues

For `*.fifo` queues, provide `groupId`, and either `deduplicationId` or enable
content-based deduplication on the queue:

```ts
await this.sqs.send("orders", {
  id: order.id,
  body: order,
  groupId: order.customerId, // ordering + parallelism unit
  deduplicationId: order.id,
});
```

### Custom serializer

Bodies are encoded before sending — strings pass through untouched, everything
else is `JSON.stringify`-ed. Override globally with `serializer`:

```ts
SqsModule.register({
  serializer: (body) => myEncode(body),
  producers: [...],
});
```

## Operational concerns

### Concurrency

Throughput per consumer is governed by `sqs-consumer` options — `batchSize`
(messages per poll) and `pollingWaitTimeMs`. To process one queue with more
parallelism, register **multiple consumers** (distinct `name`s) pointing at the
same `queueUrl`; each runs its own polling loop.

### Dead-letter queues

DLQs are configured on the queue itself (redrive policy / `maxReceiveCount`), not
in this library. Register the DLQ as its own consumer to process dead letters:

```ts
consumers: [
  { name: "orders", queueUrl: ".../orders" },
  { name: "orders-dlq", queueUrl: ".../orders-dead" },
];
```

### Graceful shutdown

Consumers start polling on `onApplicationBootstrap` and stop on
`onModuleDestroy`. Enable Nest shutdown hooks so this runs on `SIGTERM`:

```ts
const app = await NestFactory.create(AppModule);
app.enableShutdownHooks();
```

Pass `stopOptions` per consumer (or `globalStopOptions` for all) to control the
stop — e.g. `{ abort: true }` to abort in-flight requests instead of draining:

```ts
SqsModule.register({
  globalStopOptions: { abort: true },
  consumers: [{ name: "orders", queueUrl: "...", stopOptions: { abort: false } }],
});
```

### Custom logger

Pass any `LoggerService` (e.g. `nestjs-pino`) via `logger`:

```ts
SqsModule.registerAsync({
  imports: [LoggerModule],
  inject: [Logger],
  useFactory: (logger: Logger) => ({ logger, consumers: [...], producers: [...] }),
});
```

Because handlers are resolved at call time (not bound at startup), method-wrapping
decorators applied during bootstrap — tracing, metrics, correlation-id logging —
take effect on your SQS handlers.

## SqsService API

| Method                          | Returns              | Description                                      |
| ------------------------------- | -------------------- | ------------------------------------------------ |
| `send(name, message[])`         | `Promise`            | Send one or many messages to a producer.         |
| `getProducerQueueSize(name)`    | `number`             | In-flight message count buffered by the producer.|
| `purgeQueue(name)`              | `Promise`            | Purge all messages from the queue.               |
| `getQueueAttributes(name)`      | `Promise<attributes>`| Fetch all SQS queue attributes.                  |
| `consumers` / `producers`       | `Map`                | The registered instances, keyed by `name`.       |

## Testing

Unit-test your handler providers directly — they are plain NestJS providers, so
you can call the decorated method with a fake `Message`. For end-to-end tests,
point the consumers/producers at a local [ElasticMQ](https://github.com/softwaremill/elasticmq)
by passing an `SQSClient` configured with its `endpoint`:

```ts
const sqs = new SQSClient({ endpoint: "http://localhost:9324", region: "us-east-1", credentials: { accessKeyId: "x", secretAccessKey: "x" } });

SqsModule.register({
  consumers: [{ name: "orders", queueUrl: "http://localhost:9324/000/orders", sqs }],
  producers: [{ name: "orders", queueUrl: "http://localhost:9324/000/orders", sqs }],
});
```

## License

This project is licensed under the terms of the MIT license.
