import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ALL_CONSUMERS, SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD } from '../lib/sqs.constants';
import { defineQueue } from '../lib/sqs.contract';
import type { Message, SqsOptions } from '../lib/sqs.types';

// --- Mock the underlying bbc libraries so no network/broker is involved. ---

type FakeConsumer = {
  options: Record<string, unknown>;
  listeners: Array<{ event: string; handler: (...args: unknown[]) => unknown }>;
  start: ReturnType<typeof vi.fn>;
  stop: ReturnType<typeof vi.fn>;
  on: (event: string, handler: (...args: unknown[]) => unknown) => void;
  sqs: unknown;
  queueUrl: unknown;
};

const consumerInstances: FakeConsumer[] = [];
const producerInstances: Array<Record<string, unknown>> = [];

vi.mock('sqs-consumer', () => ({
  Consumer: {
    create: vi.fn((options: Record<string, unknown>) => {
      const instance: FakeConsumer = {
        options,
        listeners: [],
        start: vi.fn(),
        stop: vi.fn(),
        on(event, handler) {
          this.listeners.push({ event, handler });
        },
        sqs: options.sqs,
        queueUrl: options.queueUrl,
      };
      consumerInstances.push(instance);
      return instance;
    }),
  },
}));

vi.mock('sqs-producer', () => ({
  Producer: {
    create: vi.fn((options: Record<string, unknown>) => {
      const instance = {
        options,
        send: vi.fn().mockResolvedValue([]),
        queueSize: vi.fn().mockReturnValue(0),
        sqs: options.sqs,
        queueUrl: options.queueUrl,
      };
      producerInstances.push(instance);
      return instance;
    }),
  },
}));

// Import after mocks are registered (vi.mock is hoisted, but keep it explicit).
const { SqsService } = await import('../lib/sqs.service');

type DiscoveredMethod = {
  meta: Record<string, unknown>;
  discoveredMethod: {
    methodName: string;
    handler: (...args: unknown[]) => unknown;
    parentClass: { instance: Record<string, unknown> };
  };
};

// Build a discovered method the way @golevelup/nestjs-discovery exposes it: the
// handler lives on the provider instance under `methodName` and is resolved
// there at call time, so replacing it later (late wrapping) is honored.
function discovered(
  meta: Record<string, unknown>,
  handler: (...args: unknown[]) => unknown,
  instance: Record<string, unknown> = {},
  methodName = 'handle',
): DiscoveredMethod {
  instance[methodName] = handler;
  return { meta, discoveredMethod: { methodName, handler, parentClass: { instance } } };
}

function makeDiscover(messageHandlers: DiscoveredMethod[], eventHandlers: DiscoveredMethod[]) {
  return {
    providerMethodsWithMetaAtKey: vi.fn(async (key: symbol) => {
      if (key === SQS_CONSUMER_METHOD) return messageHandlers;
      if (key === SQS_CONSUMER_EVENT_HANDLER) return eventHandlers;
      return [];
    }),
  };
}

// biome-ignore lint/suspicious/noExplicitAny: test doubles intentionally loose.
const buildService = (options: SqsOptions, discover: any) => new SqsService(options, discover);

beforeEach(() => {
  consumerInstances.length = 0;
  producerInstances.length = 0;
  vi.clearAllMocks();
});

describe('SqsService — consumer/producer wiring', () => {
  it('registers a consumer and starts it only on application bootstrap (#108)', async () => {
    const discover = makeDiscover([discovered({ name: 'queue-a' }, vi.fn())], []);

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    expect(service.consumers.has('queue-a')).toBe(true);
    expect(consumerInstances).toHaveLength(1);
    expect(consumerInstances[0].options.handleMessage).toBeTypeOf('function');
    expect(consumerInstances[0].options.handleMessageBatch).toBeUndefined();
    // Polling must NOT start during init — only once the app is bootstrapped.
    expect(consumerInstances[0].start).not.toHaveBeenCalled();

    service.onApplicationBootstrap();
    expect(consumerInstances[0].start).toHaveBeenCalledOnce();
  });

  it('resolves the handler at call time so late method wrapping is honored (#108)', async () => {
    const calls: string[] = [];
    const instance: Record<string, unknown> = {
      handle: () => calls.push('original'),
    };
    const discover = makeDiscover([discovered({ name: 'queue-a' }, instance.handle as () => void, instance)], []);

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    // Simulate a bootstrap-time decorator replacing the method on the instance
    // AFTER wiring but BEFORE the first message is handled.
    instance.handle = () => calls.push('wrapped');
    service.onApplicationBootstrap();

    await (consumerInstances[0].options.handleMessage as (m: unknown) => unknown)({});
    expect(calls).toEqual(['wrapped']);
  });

  it('defaults alwaysAcknowledge to true so void handlers ack on success (#99)', async () => {
    const discover = makeDiscover([discovered({ name: 'queue-a' }, vi.fn())], []);

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    expect(consumerInstances[0].options.alwaysAcknowledge).toBe(true);
  });

  it('lets a consumer opt out of auto-ack with alwaysAcknowledge: false (#103)', async () => {
    const discover = makeDiscover([discovered({ name: 'queue-a' }, vi.fn())], []);

    const service = buildService(
      { consumers: [{ name: 'queue-a', queueUrl: 'url-a', alwaysAcknowledge: false } as never] },
      discover,
    );
    await service.onModuleInit();

    // The user-provided value must win over the library default.
    expect(consumerInstances[0].options.alwaysAcknowledge).toBe(false);
  });

  it('wires handleMessageBatch when the handler is declared as batch', async () => {
    const discover = makeDiscover([discovered({ name: 'queue-a', batch: true }, vi.fn())], []);

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    expect(consumerInstances[0].options.handleMessageBatch).toBeTypeOf('function');
    expect(consumerInstances[0].options.handleMessage).toBeUndefined();
  });

  it('passes the deserialized body to the handler when a deserializer is set', async () => {
    let received: unknown;
    const discover = makeDiscover(
      [
        discovered({ name: 'queue-a' }, (v: unknown) => {
          received = v;
        }),
      ],
      [],
    );
    const deserializer = (m: { Body?: string }) => JSON.parse(m.Body ?? '{}');

    const service = buildService(
      { consumers: [{ name: 'queue-a', queueUrl: 'url-a', deserializer } as never] },
      discover,
    );
    await service.onModuleInit();

    await (consumerInstances[0].options.handleMessage as (m: unknown) => unknown)({ Body: '{"id":7}' });
    expect(received).toEqual({ id: 7 });
  });

  it('applies the deserializer per message for batch handlers', async () => {
    let received: unknown;
    const discover = makeDiscover(
      [
        discovered({ name: 'queue-a', batch: true }, (v: unknown) => {
          received = v;
        }),
      ],
      [],
    );
    const deserializer = (m: { Body?: string }) => JSON.parse(m.Body ?? '{}');

    const service = buildService(
      { consumers: [{ name: 'queue-a', queueUrl: 'url-a', deserializer } as never] },
      discover,
    );
    await service.onModuleInit();

    await (consumerInstances[0].options.handleMessageBatch as (m: unknown[]) => unknown)([
      { Body: '{"i":1}' },
      { Body: '{"i":2}' },
    ]);
    expect(received).toEqual([{ i: 1 }, { i: 2 }]);
  });

  it('passes the raw message to the handler when no deserializer is set', async () => {
    let received: unknown;
    const discover = makeDiscover(
      [
        discovered({ name: 'queue-a' }, (v: unknown) => {
          received = v;
        }),
      ],
      [],
    );

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    const raw = { Body: '{"id":7}' };
    await (consumerInstances[0].options.handleMessage as (m: unknown) => unknown)(raw);
    expect(received).toBe(raw);
  });

  it('propagates a throwing deserializer so the message is not acknowledged', async () => {
    const handler = vi.fn();
    const discover = makeDiscover([discovered({ name: 'queue-a' }, handler)], []);
    const deserializer = () => {
      throw new Error('invalid payload');
    };

    const service = buildService(
      { consumers: [{ name: 'queue-a', queueUrl: 'url-a', deserializer } as never] },
      discover,
    );
    await service.onModuleInit();

    const run = consumerInstances[0].options.handleMessage as (m: unknown) => unknown;
    expect(() => run({ Body: 'x' })).toThrow('invalid payload');
    expect(handler).not.toHaveBeenCalled();
  });

  it('throws when two consumers share the same name', async () => {
    const discover = makeDiscover([discovered({ name: 'dup' }, vi.fn())], []);

    const service = buildService(
      {
        consumers: [{ name: 'dup', queueUrl: 'url-1' } as never, { name: 'dup', queueUrl: 'url-2' } as never],
      },
      discover,
    );

    await expect(service.onModuleInit()).rejects.toThrow('Consumer already exists: dup');
  });

  it('skips (with a warning) a configured consumer that has no message handler', async () => {
    const warn = vi.fn();
    const discover = makeDiscover([], []);

    const service = buildService(
      { consumers: [{ name: 'orphan', queueUrl: 'url' } as never], logger: { warn } as never },
      discover,
    );
    await service.onModuleInit();

    expect(service.consumers.has('orphan')).toBe(false);
    expect(warn).toHaveBeenCalledWith('No metadata found for: orphan');
  });

  // Regression guard for B1: the event handler must be bound to the provider
  // that declares it, not to the message handler's provider.
  it('binds the event handler to the class that declares it', async () => {
    const messageInstance = { kind: 'message-owner' };
    const eventInstance = { kind: 'event-owner' };
    let boundThis: unknown;

    const eventHandler = function (this: unknown) {
      boundThis = this;
    };

    const discover = makeDiscover(
      [discovered({ name: 'queue-a' }, vi.fn(), messageInstance)],
      [discovered({ name: 'queue-a', eventName: 'processing_error' }, eventHandler, eventInstance)],
    );

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    const registered = consumerInstances[0].listeners.find((l) => l.event === 'processing_error');
    expect(registered).toBeDefined();
    registered?.handler();

    // The event handler must run with `this` bound to the provider that declares
    // it, not to the message-handler's provider (regression guard for B1).
    expect(boundThis).toBe(eventInstance);
  });

  it('attaches an ALL_CONSUMERS event handler to every consumer (#89)', async () => {
    const seen: string[] = [];
    const globalHandler = (payload: unknown) => seen.push(payload as string);

    const discover = makeDiscover(
      [discovered({ name: 'queue-a' }, vi.fn()), discovered({ name: 'queue-b' }, vi.fn())],
      [discovered({ name: ALL_CONSUMERS, eventName: 'processing_error' }, globalHandler)],
    );

    const service = buildService(
      {
        consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never, { name: 'queue-b', queueUrl: 'url-b' } as never],
      },
      discover,
    );
    await service.onModuleInit();

    // The catch-all handler must be wired on both consumers.
    expect(consumerInstances).toHaveLength(2);
    for (const consumer of consumerInstances) {
      const registered = consumer.listeners.find((l) => l.event === 'processing_error');
      expect(registered).toBeDefined();
      registered?.handler(`from-${consumer.queueUrl}`);
    }
    expect(seen).toEqual(['from-url-a', 'from-url-b']);
  });

  it('stops each consumer with its resolved stop options on destroy', async () => {
    const discover = makeDiscover([discovered({ name: 'queue-a' }, vi.fn())], []);

    const service = buildService(
      {
        consumers: [{ name: 'queue-a', queueUrl: 'url-a', stopOptions: { abort: true } } as never],
      },
      discover,
    );
    await service.onModuleInit();
    service.onModuleDestroy();

    expect(consumerInstances[0].stop).toHaveBeenCalledWith({ abort: true });
  });
});

describe('SqsService — producer API', () => {
  const discover = makeDiscover([], []);

  it('registers producers and reports queue size', async () => {
    const service = buildService({ producers: [{ name: 'p', queueUrl: 'url' } as never] }, discover);
    await service.onModuleInit();

    expect(service.producers.has('p')).toBe(true);
    expect(service.getProducerQueueSize('p')).toBe(0);
  });

  it('serializes non-string bodies to JSON and passes strings through', async () => {
    const service = buildService({ producers: [{ name: 'p', queueUrl: 'url' } as never] }, discover);
    await service.onModuleInit();

    const payload: Message[] = [
      { id: '1', body: { hello: 'world' } },
      { id: '2', body: 'already-a-string' },
    ];
    await service.send('p', payload);

    const producer = producerInstances[0];
    const sent = (producer.send as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(sent[0].body).toBe(JSON.stringify({ hello: 'world' }));
    expect(sent[1].body).toBe('already-a-string');
  });

  it('uses a custom serializer when provided', async () => {
    const serializer = vi.fn((body: unknown) => `custom:${JSON.stringify(body)}`);
    const service = buildService({ producers: [{ name: 'p', queueUrl: 'url' } as never], serializer }, discover);
    await service.onModuleInit();

    await service.send('p', { id: '1', body: { a: 1 } });

    const sent = (producerInstances[0].send as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(serializer).toHaveBeenCalledWith({ a: 1 });
    expect(sent[0].body).toBe('custom:{"a":1}');
  });

  it('resolves the producer name from a typed queue contract', async () => {
    const orders = defineQueue({ name: 'orders', schema: { parse: (input: unknown) => input } });
    const service = buildService({ producers: [orders.producer({ queueUrl: 'url' } as never)] }, discover);
    await service.onModuleInit();

    await service.send(orders, { id: '1', body: { a: 1 } });

    const sent = (producerInstances[0].send as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(sent[0].body).toBe(JSON.stringify({ a: 1 }));
  });

  it('throws when sending to an unknown producer', async () => {
    const service = buildService({ producers: [] }, discover);
    await service.onModuleInit();

    // The guard throws synchronously, before any promise is created.
    expect(() => service.send('missing', { id: '1', body: {} })).toThrow('Producer does not exist: missing');
  });

  it('throws when asking for the size of an unknown producer', async () => {
    const service = buildService({ producers: [] }, discover);
    await service.onModuleInit();

    expect(() => service.getProducerQueueSize('missing')).toThrow('Producer does not exist: missing');
  });

  it('lists the registered producer names in the not-found error', async () => {
    const service = buildService(
      { producers: [{ name: 'orders', queueUrl: 'url' } as never, { name: 'emails', queueUrl: 'url' } as never] },
      discover,
    );
    await service.onModuleInit();

    expect(() => service.getProducerQueueSize('nope')).toThrow('Registered producers: orders, emails');
  });
});

describe('SqsService — queue admin', () => {
  const discover = makeDiscover([], []);

  it('purges a queue via the resolved sqs client', async () => {
    const send = vi.fn().mockResolvedValue({});
    const service = buildService(
      { producers: [{ name: 'p', queueUrl: 'https://sqs/url', sqs: { send } } as never] },
      discover,
    );
    await service.onModuleInit();

    await service.purgeQueue('p');

    expect(send).toHaveBeenCalledOnce();
    expect(send.mock.calls[0][0].input).toEqual({ QueueUrl: 'https://sqs/url' });
  });

  it('reads queue attributes via the resolved sqs client', async () => {
    const send = vi.fn().mockResolvedValue({ Attributes: { ApproximateNumberOfMessages: '5' } });
    const service = buildService(
      { producers: [{ name: 'p', queueUrl: 'https://sqs/url', sqs: { send } } as never] },
      discover,
    );
    await service.onModuleInit();

    const attributes = await service.getQueueAttributes('p');

    expect(attributes).toEqual({ ApproximateNumberOfMessages: '5' });
    expect(send.mock.calls[0][0].input).toEqual({ QueueUrl: 'https://sqs/url', AttributeNames: ['All'] });
  });

  it('throws with registered names when the queue is unknown', async () => {
    const service = buildService(
      { producers: [{ name: 'p', queueUrl: 'url', sqs: { send: vi.fn() } } as never] },
      discover,
    );
    await service.onModuleInit();

    await expect(service.purgeQueue('nope')).rejects.toThrow(
      'Consumer/Producer does not exist: nope. Registered consumers: (none); producers: p',
    );
  });
});
