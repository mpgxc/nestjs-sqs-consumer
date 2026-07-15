import type { MessageAttributeValue, Message as SqsRawMessage } from '@aws-sdk/client-sqs';
import type { LoggerService, ModuleMetadata, Type } from '@nestjs/common';
import type { Consumer, ConsumerOptions, StopOptions } from 'sqs-consumer';
import type { Producer } from 'sqs-producer';

export type ProducerOptions = Parameters<typeof Producer.create>[0];
export type QueueName = string;

export type SqsConsumerOptions = Omit<ConsumerOptions, 'handleMessage' | 'handleMessageBatch'> & {
  name: QueueName;
  stopOptions?: StopOptions;
  /**
   * Transforms each raw SQS message before it reaches the handler. When set, the
   * `@SqsMessageHandler` receives the returned value instead of the raw
   * `Message` (for batch handlers it is applied per message, so the handler
   * receives an array of results). Throwing here — e.g. a Zod `.parse` failure —
   * propagates as a processing error, so the message is not acknowledged and is
   * redelivered / dead-lettered.
   */
  deserializer?: (message: SqsRawMessage) => unknown;
  /**
   * Extracts a discriminator value from each message so it can be routed to the
   * `@SqsMessageHandler({ name, type })` whose `type` matches. Required when the
   * queue has any typed handlers. See `byBodyField` / `byMessageAttribute`.
   */
  discriminator?: (message: SqsRawMessage) => string | undefined;
  /**
   * What to do with a message whose discriminator matches no typed handler (and
   * there is no untyped fallback handler). `'error'` (default) throws, so the
   * message is not acknowledged and is redelivered / dead-lettered; `'ignore'`
   * logs a warning and acknowledges it.
   */
  onUnmatched?: 'error' | 'ignore';
};

export type SqsConsumerMapValues = {
  instance: Consumer;
  stopOptions: StopOptions;
};

export type SqsProducerOptions = ProducerOptions & {
  name: QueueName;
};

export interface SqsOptions {
  consumers?: SqsConsumerOptions[];
  producers?: SqsProducerOptions[];
  logger?: LoggerService;
  globalStopOptions?: StopOptions;
  /**
   * Controls how a non-string message body is encoded before being sent.
   * Defaults to: strings pass through untouched, everything else is
   * `JSON.stringify`-ed.
   */
  serializer?: (body: unknown) => string;
}

export interface SqsModuleOptionsFactory {
  createOptions(): Promise<SqsOptions> | SqsOptions;
}

export interface SqsModuleAsyncOptions extends Pick<ModuleMetadata, 'imports' | 'providers'> {
  useExisting?: Type<SqsModuleOptionsFactory>;
  useClass?: Type<SqsModuleOptionsFactory>;
  useFactory?: (...args: any[]) => Promise<SqsOptions> | SqsOptions;
  inject?: any[];
}

export interface Message<T = any> {
  id: string;
  body: T;
  groupId?: string;
  deduplicationId?: string;
  delaySeconds?: number;
  messageAttributes?: Record<string, MessageAttributeValue>;
}

export interface SqsMessageHandlerMeta {
  name: string;
  batch?: boolean;
  /**
   * Routes only messages whose discriminator (see `SqsConsumerOptions.discriminator`)
   * equals this value to the decorated method. Omit for a single whole-queue
   * handler, or for the fallback handler when other handlers on the queue are typed.
   */
  type?: string;
}

export interface SqsConsumerEventHandlerMeta {
  name: string;
  eventName: string;
}
