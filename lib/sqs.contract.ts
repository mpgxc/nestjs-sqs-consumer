import type { Message as SqsRawMessage } from '@aws-sdk/client-sqs';
import type { SqsConsumerOptions, SqsProducerOptions } from './sqs.types';

/**
 * The minimal shape a schema needs to be used as a message contract. It is
 * satisfied structurally by a Zod schema (`schema.parse(data: unknown): T`), so
 * this library takes no dependency on any particular validation library.
 */
export interface MessageSchema<T> {
  parse(input: unknown): T;
}

/**
 * A typed queue contract produced by {@link defineQueue}. It binds a `name` to a
 * `schema` so the same schema drives runtime validation, the handler's payload
 * type, and the producer's body type.
 */
export interface SqsQueue<Name extends string, T> {
  readonly name: Name;
  readonly schema: MessageSchema<T>;
  /** Consumer options for this queue, with `name` and the schema `deserializer` prefilled. */
  consumer(options: Omit<SqsConsumerOptions, 'name' | 'deserializer'>): SqsConsumerOptions;
  /** Producer options for this queue, with `name` prefilled. */
  producer(options: Omit<SqsProducerOptions, 'name'>): SqsProducerOptions;
}

/** Extracts the payload type carried by an {@link SqsQueue} — use to type your handler. */
export type Payload<Q> = Q extends SqsQueue<string, infer T> ? T : never;

/**
 * Define a strongly-typed queue from a `name` and a `schema`.
 *
 * ```ts
 * const OrdersQueue = defineQueue({ name: "orders", schema: OrderSchema });
 *
 * // registration — deserializer is wired from the schema automatically
 * consumers: [OrdersQueue.consumer({ queueUrl })];
 * producers: [OrdersQueue.producer({ queueUrl })];
 *
 * // handler — payload type derived from the same schema, no manual z.infer
 * @SqsMessageHandler({ name: OrdersQueue.name })
 * handle(order: Payload<typeof OrdersQueue>) {}
 *
 * // producing — body is type-checked against the schema
 * sqs.send(OrdersQueue, { id, body: order });
 * ```
 *
 * By default the raw message `Body` is `JSON.parse`-d before being handed to the
 * schema; override `decode` for a different wire format.
 */
export function defineQueue<Name extends string, T>(config: {
  name: Name;
  schema: MessageSchema<T>;
  decode?: (message: SqsRawMessage) => unknown;
}): SqsQueue<Name, T> {
  const decode = config.decode ?? ((message: SqsRawMessage) => JSON.parse(message.Body ?? 'null'));
  return {
    name: config.name,
    schema: config.schema,
    consumer(options) {
      return {
        ...options,
        name: config.name,
        deserializer: (message: SqsRawMessage) => config.schema.parse(decode(message)),
      };
    },
    producer(options) {
      return { ...options, name: config.name };
    },
  };
}
