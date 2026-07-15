import type { EventEmitter } from 'node:events';
import type { QueueAttributeName, SQSClient, Message as SqsRawMessage } from '@aws-sdk/client-sqs';
import { GetQueueAttributesCommand, PurgeQueueCommand } from '@aws-sdk/client-sqs';
import { DiscoveryService } from '@golevelup/nestjs-discovery';
import type { LoggerService, OnApplicationBootstrap, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { StopOptions } from 'sqs-consumer';
import { Consumer } from 'sqs-consumer';
import { Producer } from 'sqs-producer';
import { ALL_CONSUMERS, SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD, SQS_OPTIONS } from './sqs.constants';
import type { SqsQueue } from './sqs.contract';
import type {
  Message,
  QueueName,
  SqsConsumerEventHandlerMeta,
  SqsConsumerMapValues,
  SqsMessageHandlerMeta,
  SqsOptions,
} from './sqs.types';

@Injectable()
export class SqsService implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy {
  public readonly consumers = new Map<QueueName, SqsConsumerMapValues>();
  public readonly producers = new Map<QueueName, Producer>();

  private logger!: LoggerService;
  private globalStopOptions!: StopOptions;
  private readonly serialize: (body: unknown) => string;

  public constructor(
    @Inject(SQS_OPTIONS) public readonly options: SqsOptions,
    @Inject(DiscoveryService) private readonly discover: DiscoveryService,
  ) {
    this.serialize =
      this.options.serializer ?? ((body: unknown) => (typeof body === 'string' ? body : JSON.stringify(body)));
  }

  public async onModuleInit(): Promise<void> {
    this.logger = this.options.logger ?? new Logger('SqsService', { timestamp: false });
    this.globalStopOptions = this.options.globalStopOptions ?? {};

    const messageHandlers =
      await this.discover.providerMethodsWithMetaAtKey<SqsMessageHandlerMeta>(SQS_CONSUMER_METHOD);
    const eventHandlers =
      await this.discover.providerMethodsWithMetaAtKey<SqsConsumerEventHandlerMeta>(SQS_CONSUMER_EVENT_HANDLER);

    this.options.consumers?.forEach((options) => {
      const { name, stopOptions, deserializer, discriminator, onUnmatched, ...consumerOptions } = options;
      if (this.consumers.has(name)) {
        throw new Error(`Consumer already exists: ${name}`);
      }

      const handlers = messageHandlers.filter(({ meta }) => meta.name === name);
      if (handlers.length === 0) {
        this.logger.warn(`No metadata found for: ${name}`);
        return;
      }

      const routed = handlers.filter(({ meta }) => meta.type !== undefined);
      // biome-ignore lint/suspicious/noExplicitAny: matches sqs-consumer's loose handler contract.
      let handleMessage: ((...args: any[]) => any) | undefined;
      // biome-ignore lint/suspicious/noExplicitAny: matches sqs-consumer's loose handler contract.
      let handleMessageBatch: ((...args: any[]) => any) | undefined;

      if (routed.length > 0) {
        handleMessage = this.buildRouter(name, handlers, { discriminator, deserializer, onUnmatched });
      } else {
        const metadata = handlers[0];
        const isBatchHandler = metadata.meta.batch === true;
        const handler = this.withDeserializer(this.lateBound(metadata.discoveredMethod), deserializer, isBatchHandler);
        if (isBatchHandler) {
          handleMessageBatch = handler;
        } else {
          handleMessage = handler;
        }
      }

      const consumer = Consumer.create({
        // Default to acknowledging a message once its handler resolves without
        // throwing, preserving the pre-v4 nestjs-sqs behavior. Since sqs-consumer
        // v15 no longer acknowledges on an `undefined` return, an opt-out is a
        // per-consumer `alwaysAcknowledge: false` (return the message to ack).
        alwaysAcknowledge: true,
        ...consumerOptions,
        ...(handleMessageBatch ? { handleMessageBatch } : { handleMessage }),
      });

      // A handler registered for this queue's name, plus any catch-all handler
      // registered with the ALL_CONSUMERS wildcard, are attached here. (#89)
      const eventsMetadata = eventHandlers.filter(({ meta }) => meta.name === name || meta.name === ALL_CONSUMERS);
      for (const eventMetadata of eventsMetadata) {
        // sqs-consumer v15 types events strictly (`on<E extends keyof Events>`),
        // but handlers are discovered with runtime-only event names — attach via
        // the underlying Node EventEmitter that Consumer extends.
        (consumer as EventEmitter).on(eventMetadata.meta.eventName, this.lateBound(eventMetadata.discoveredMethod));
      }
      this.consumers.set(name, { instance: consumer, stopOptions: stopOptions ?? this.globalStopOptions });
    });

    this.options.producers?.forEach((options) => {
      const { name, ...producerOptions } = options;
      if (this.producers.has(name)) {
        throw new Error(`Producer already exists: ${name}`);
      }

      const producer = Producer.create(producerOptions);
      this.producers.set(name, producer);
    });
  }

  /**
   * Consumers are wired during `onModuleInit`, but polling only starts here — in
   * `onApplicationBootstrap`, which Nest guarantees to run after every module's
   * init hook. This is what makes lazy handler resolution meaningful: any
   * decorator that wraps a handler method during bootstrap (tracing, metrics,
   * `@Transactional`, etc.) is already in place before the first message is
   * pulled, and is picked up because handlers are resolved at call time. (#108)
   */
  public onApplicationBootstrap(): void {
    for (const consumer of this.consumers.values()) {
      consumer.instance.start();
    }
  }

  public onModuleDestroy() {
    for (const consumer of this.consumers.values()) {
      consumer.instance.stop(consumer.stopOptions);
    }
  }

  /**
   * Wrap a discovered handler in a closure that resolves the method on its
   * provider instance at call time, instead of binding a snapshot at init time.
   * A method replaced later (e.g. wrapped by an interceptor/observability
   * decorator during bootstrap) is therefore honored, and `this` stays bound to
   * the declaring provider. (#108)
   */
  private lateBound(discoveredMethod: {
    methodName: string;
    parentClass: { instance: unknown };
    // biome-ignore lint/suspicious/noExplicitAny: matches sqs-consumer's loose handler contract.
  }): (...args: any[]) => any {
    const { instance } = discoveredMethod.parentClass;
    const { methodName } = discoveredMethod;
    // biome-ignore lint/suspicious/noExplicitAny: dynamic method dispatch on the provider instance.
    return (...args: any[]) => (instance as Record<string, (...a: any[]) => any>)[methodName](...args);
  }

  /**
   * If a `deserializer` is configured for a consumer, wrap the handler so it
   * receives the parsed value instead of the raw SQS message (applied per
   * message for batch handlers). A throwing deserializer propagates, so the
   * message is not acknowledged and is redelivered / dead-lettered.
   */
  private withDeserializer(
    // biome-ignore lint/suspicious/noExplicitAny: matches sqs-consumer's loose handler contract.
    invoke: (...args: any[]) => any,
    deserializer: ((message: SqsRawMessage) => unknown) | undefined,
    isBatch: boolean,
    // biome-ignore lint/suspicious/noExplicitAny: matches sqs-consumer's loose handler contract.
  ): (...args: any[]) => any {
    if (!deserializer) {
      return invoke;
    }
    return isBatch
      ? (messages: SqsRawMessage[]) => invoke(messages.map((message) => deserializer(message)))
      : (message: SqsRawMessage) => invoke(deserializer(message));
  }

  /**
   * Build a per-message dispatcher for a queue that has typed
   * `@SqsMessageHandler({ name, type })` handlers. Each message's discriminator
   * selects the matching handler; an untyped handler on the same queue, if any,
   * is the fallback. Unmatched messages follow `onUnmatched` ('error' by
   * default → not acknowledged → dead-lettered).
   */
  private buildRouter(
    name: QueueName,
    handlers: Array<{
      meta: SqsMessageHandlerMeta;
      discoveredMethod: { methodName: string; parentClass: { instance: unknown } };
    }>,
    options: {
      discriminator?: (message: SqsRawMessage) => string | undefined;
      deserializer?: (message: SqsRawMessage) => unknown;
      onUnmatched?: 'error' | 'ignore';
    },
  ): (message: SqsRawMessage) => unknown {
    const { discriminator, deserializer, onUnmatched } = options;
    if (!discriminator) {
      throw new Error(`Consumer "${name}" has typed @SqsMessageHandler(s) but no "discriminator" was configured`);
    }

    const routes = new Map<string, (arg: unknown) => unknown>();
    for (const handler of handlers) {
      if (handler.meta.type !== undefined) {
        routes.set(handler.meta.type, this.lateBound(handler.discoveredMethod));
      }
    }
    const fallback = handlers.find(({ meta }) => meta.type === undefined);
    const fallbackFn = fallback ? this.lateBound(fallback.discoveredMethod) : undefined;

    return (message: SqsRawMessage) => {
      const key = discriminator(message);
      const target = (key !== undefined ? routes.get(key) : undefined) ?? fallbackFn;
      if (!target) {
        if (onUnmatched === 'ignore') {
          this.logger.warn(`No @SqsMessageHandler for type "${key}" on consumer "${name}"; ignoring message`);
          return undefined;
        }
        throw new Error(`No @SqsMessageHandler for message type "${key}" on consumer "${name}"`);
      }
      return target(deserializer ? deserializer(message) : message);
    };
  }

  private knownNames(map: Map<QueueName, unknown>): string {
    const names = [...map.keys()];
    return names.length ? names.join(', ') : '(none)';
  }

  private getQueueInfo(name: QueueName) {
    if (!this.consumers.has(name) && !this.producers.has(name)) {
      throw new Error(
        `Consumer/Producer does not exist: ${name}. ` +
          `Registered consumers: ${this.knownNames(this.consumers)}; producers: ${this.knownNames(this.producers)}`,
      );
    }

    const { sqs, queueUrl } = (this.consumers.get(name)?.instance ?? this.producers.get(name)) as {
      sqs: SQSClient;
      queueUrl: string;
    };
    if (!sqs) {
      throw new Error('SQS instance does not exist');
    }

    return {
      sqs,
      queueUrl,
    };
  }

  public async purgeQueue(name: QueueName) {
    const { sqs, queueUrl } = this.getQueueInfo(name);
    const command = new PurgeQueueCommand({
      QueueUrl: queueUrl,
    });
    return await sqs.send(command);
  }

  public async getQueueAttributes(name: QueueName) {
    const { sqs, queueUrl } = this.getQueueInfo(name);
    const command = new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['All'],
    });
    const response = await sqs.send(command);
    return response.Attributes as { [key in QueueAttributeName]: string };
  }

  public getProducerQueueSize(name: QueueName) {
    const producer = this.producers.get(name);
    if (!producer) {
      throw new Error(`Producer does not exist: ${name}. Registered producers: ${this.knownNames(this.producers)}`);
    }

    return producer.queueSize();
  }

  public send<T>(queue: SqsQueue<string, T>, message: Message<T> | Message<T>[]): ReturnType<Producer['send']>;
  public send<T = any>(name: QueueName, payload: Message<T> | Message<T>[]): ReturnType<Producer['send']>;
  public send<T = any>(
    target: QueueName | SqsQueue<string, T>,
    payload: Message<T> | Message<T>[],
  ): ReturnType<Producer['send']> {
    const name = typeof target === 'string' ? target : target.name;
    const producer = this.producers.get(name);
    if (!producer) {
      throw new Error(`Producer does not exist: ${name}. Registered producers: ${this.knownNames(this.producers)}`);
    }

    const originalMessages = Array.isArray(payload) ? payload : [payload];
    const messages = originalMessages.map((message) => ({
      ...message,
      body: this.serialize(message.body),
    }));

    return producer.send(messages as any[]);
  }
}
