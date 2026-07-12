# nestjs-sqs

[![Test](https://github.com/ssut/nestjs-sqs/workflows/Test/badge.svg)](https://github.com/ssut/nestjs-sqs/actions?query=workflow%3ATest)
[![npm version](https://badge.fury.io/js/%40ssut%2Fnestjs-sqs.svg)](https://badge.fury.io/js/%40ssut%2Fnestjs-sqs)

Tested with: [AWS SQS](https://aws.amazon.com/en/sqs/) and [ElasticMQ](https://github.com/softwaremill/elasticmq).

Nestjs-sqs is a project to make SQS easier to use and control some required flows with NestJS.
This module provides decorator-based message handling suited for simple use.

This library internally uses [bbc/sqs-producer](https://github.com/bbc/sqs-producer) and [bbc/sqs-consumer](https://github.com/bbc/sqs-consumer), and implements some more useful features on top of the basic functionality given by them.

## Requirements

- **Node.js >= 22** (the underlying `sqs-consumer` v15 / `sqs-producer` v9 track active LTS only).
- **NestJS >= 6** (tested against v11).
- **`@aws-sdk/client-sqs` >= 3.1036** as a peer dependency (AWS SDK v3).

## Installation

```shell script
npm i --save @ssut/nestjs-sqs @aws-sdk/client-sqs
```

## Quick Start

### Register module

Just register this module:

```ts
@Module({
  imports: [
    SqsModule.register({
      consumers: [
        {
          // Name is a unique identifier for this consumer instance
          name: "myConsumer1",
          // The actual SQS queue URL
          queueUrl: "https://sqs.region.amazonaws.com/account/queue-name",
          region: "us-east-1",
        },
      ],
      producers: [
        {
          // Name is a unique identifier for this producer instance
          name: "myProducer1",
          // The actual SQS queue URL
          queueUrl: "https://sqs.region.amazonaws.com/account/queue-name",
          region: "us-east-1",
        },
      ],
    }),
  ],
})
class AppModule {}
```

Quite often you might want to asynchronously pass module options instead of passing them beforehand.
In such case, use `registerAsync()` method like many other Nest.js libraries.

- Use factory

```ts
SqsModule.registerAsync({
  useFactory: () => {
    return {
      consumers: [...],
      producers: [...],
    };
  },
});
```

- Use class

```ts
SqsModule.registerAsync({
  useClass: SqsConfigService,
});
```

- Use existing

```ts
SqsModule.registerAsync({
  imports: [ConfigModule],
  useExisting: ConfigService,
});
```

### Decorate methods

You need to decorate methods in your NestJS providers in order to have them be automatically attached as event handlers for incoming SQS messages:

```ts
import { Message } from "@aws-sdk/client-sqs";

@Injectable()
export class AppMessageHandler {
  @SqsMessageHandler({ name: "myConsumer1", batch: false })
  public async handleMessage(message: Message) {}

  @SqsConsumerEventHandler({ name: "myConsumer1", eventName: "processing_error" })
  public onProcessingError(error: Error, message: Message) {
    // report errors here
  }
}
```

> The legacy positional forms — `@SqsMessageHandler("myConsumer1", false)` and
> `@SqsConsumerEventHandler("myConsumer1", "processing_error")` — still work.

#### Catch-all event handler

Use the `ALL_CONSUMERS` wildcard as the name to handle an event for **every**
consumer with a single method — useful for centralized error reporting:

```ts
import { ALL_CONSUMERS, SqsConsumerEventHandler } from "@ssut/nestjs-sqs";

@SqsConsumerEventHandler(ALL_CONSUMERS, "processing_error")
public onAnyProcessingError(error: Error, message: Message) {
  // fires for processing failures on any queue
}
```

### Produce messages

```ts
export class AppService {
  public constructor(
    private readonly sqsService: SqsService,
  ) { }

  public async dispatchSomething() {
    await this.sqsService.send(/** name: */ 'myProducer1', {
      id: 'id',
      body: { ... },
      groupId: 'groupId',
      deduplicationId: 'deduplicationId',
      messageAttributes: { ... },
      delaySeconds: 0,
    });
  }
}
```

### Message acknowledgement

By default a message is **acknowledged (deleted) once its handler resolves
without throwing** — a `void`/`async` handler that finishes successfully marks
the message as processed. Throwing (or rejecting) leaves the message on the
queue for redelivery.

This is implemented by defaulting `alwaysAcknowledge: true` on every consumer.
If you want to control acknowledgement yourself — the native `sqs-consumer`
contract, where returning `undefined` does **not** delete the message — opt out
per consumer and return the message (or an object with its `MessageId`) to ack:

```ts
SqsModule.register({
  consumers: [
    {
      name: "myConsumer1",
      queueUrl: "...",
      alwaysAcknowledge: false, // handler must return the message to ack it
    },
  ],
});
```

### Batch handling

Set `batch: true` on the handler to receive an array of messages
(`handleMessageBatch`) instead of one at a time. Tune throughput with
`sqs-consumer`'s `batchSize` (1–10) on the consumer:

```ts
// registration
consumers: [{ name: "myConsumer1", queueUrl: "...", batchSize: 10 }];

// handler
@SqsMessageHandler({ name: "myConsumer1", batch: true })
public async handleBatch(messages: Message[]) {}
```

### Custom serializer

Message bodies are encoded before being sent — strings pass through untouched,
everything else is `JSON.stringify`-ed. Override this globally with
`serializer`:

```ts
SqsModule.register({
  serializer: (body) => myEncode(body),
  producers: [...],
});
```

### Concurrency

Throughput per consumer is governed by `sqs-consumer` options — `batchSize`
(messages per poll) and `pollingWaitTimeMs`. To process a single queue with more
parallelism, register **multiple consumers** (distinct `name`s) pointing at the
same `queueUrl`; each runs its own polling loop.

### Configuration

See [here](https://github.com/ssut/nestjs-sqs/blob/master/lib/sqs.types.ts), and note that we have same configuration as [bbc/sqs-consumer's](https://github.com/bbc/sqs-consumer).
In most time you just need to specify both `name` and `queueUrl` at the minimum requirements.

## License

This project is licensed under the terms of the MIT license.
