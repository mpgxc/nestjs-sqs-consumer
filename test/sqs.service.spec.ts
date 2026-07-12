import { beforeEach, describe, expect, it, vi } from 'vitest';
import { SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD } from '../lib/sqs.constants';
import type { Message, SqsOptions } from '../lib/sqs.types';

// --- Mock the underlying bbc libraries so no network/broker is involved. ---

type FakeConsumer = {
  options: Record<string, unknown>;
  listeners: Array<{ event: string; handler: (...args: unknown[]) => unknown }>;
  start: ReturnType<typeof vi.fn>;
  stop: ReturnType<typeof vi.fn>;
  addListener: (event: string, handler: (...args: unknown[]) => unknown) => void;
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
        addListener(event, handler) {
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
    handler: (...args: unknown[]) => unknown;
    parentClass: { instance: unknown };
  };
};

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
  it('registers a consumer with the discovered message handler and starts it', async () => {
    const handler = vi.fn();
    const instance = {};
    const discover = makeDiscover(
      [{ meta: { name: 'queue-a' }, discoveredMethod: { handler, parentClass: { instance } } }],
      [],
    );

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    expect(service.consumers.has('queue-a')).toBe(true);
    expect(consumerInstances).toHaveLength(1);
    expect(consumerInstances[0].options.handleMessage).toBeTypeOf('function');
    expect(consumerInstances[0].options.handleMessageBatch).toBeUndefined();
    expect(consumerInstances[0].start).toHaveBeenCalledOnce();
  });

  it('wires handleMessageBatch when the handler is declared as batch', async () => {
    const handler = vi.fn();
    const discover = makeDiscover(
      [{ meta: { name: 'queue-a', batch: true }, discoveredMethod: { handler, parentClass: { instance: {} } } }],
      [],
    );

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    expect(consumerInstances[0].options.handleMessageBatch).toBeTypeOf('function');
    expect(consumerInstances[0].options.handleMessage).toBeUndefined();
  });

  it('throws when two consumers share the same name', async () => {
    const discover = makeDiscover(
      [{ meta: { name: 'dup' }, discoveredMethod: { handler: vi.fn(), parentClass: { instance: {} } } }],
      [],
    );

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

  // KNOWN BUG (B1): the event handler is currently bound to the *message*
  // handler's provider instance instead of its own. `it.fails` documents this
  // as an executable spec; remove `.fails` when B1 is fixed in Fase 1.
  it.fails('binds the event handler to the class that declares it', async () => {
    const messageInstance = { kind: 'message-owner' };
    const eventInstance = { kind: 'event-owner' };
    let boundThis: unknown;

    const eventHandler = function (this: unknown) {
      boundThis = this;
    };

    const discover = makeDiscover(
      [
        {
          meta: { name: 'queue-a' },
          discoveredMethod: { handler: vi.fn(), parentClass: { instance: messageInstance } },
        },
      ],
      [
        {
          meta: { name: 'queue-a', eventName: 'processing_error' },
          discoveredMethod: { handler: eventHandler, parentClass: { instance: eventInstance } },
        },
      ],
    );

    const service = buildService({ consumers: [{ name: 'queue-a', queueUrl: 'url-a' } as never] }, discover);
    await service.onModuleInit();

    const registered = consumerInstances[0].listeners.find((l) => l.event === 'processing_error');
    expect(registered).toBeDefined();
    registered?.handler();

    // The event handler must run with `this` bound to the provider that declares
    // it, not to the message-handler's provider (regression guard for #108/B1).
    expect(boundThis).toBe(eventInstance);
  });

  it('stops each consumer with its resolved stop options on destroy', async () => {
    const discover = makeDiscover(
      [{ meta: { name: 'queue-a' }, discoveredMethod: { handler: vi.fn(), parentClass: { instance: {} } } }],
      [],
    );

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
});
