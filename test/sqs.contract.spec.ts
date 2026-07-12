import { describe, expect, it, vi } from 'vitest';
import { defineQueue } from '../lib/sqs.contract';

const identity = { parse: (input: unknown) => input };

describe('defineQueue', () => {
  it('exposes the name and schema', () => {
    const queue = defineQueue({ name: 'orders', schema: identity });
    expect(queue.name).toBe('orders');
    expect(queue.schema).toBe(identity);
  });

  it('consumer() prefills the name and a schema-backed deserializer', () => {
    const queue = defineQueue({ name: 'orders', schema: { parse: (input) => ({ ok: input }) } });
    const options = queue.consumer({ queueUrl: 'url' } as never);

    expect(options.name).toBe('orders');
    const deserializer = options.deserializer;
    expect(deserializer).toBeTypeOf('function');
    expect(deserializer?.({ Body: '{"a":1}' } as never)).toEqual({ ok: { a: 1 } });
  });

  it('deserializer JSON-parses the Body before handing it to the schema', () => {
    const parse = vi.fn((input: unknown) => input);
    const queue = defineQueue({ name: 'orders', schema: { parse } });

    queue.consumer({ queueUrl: 'url' } as never).deserializer?.({ Body: '{"x":2}' } as never);

    expect(parse).toHaveBeenCalledWith({ x: 2 });
  });

  it('supports a custom decode step', () => {
    const queue = defineQueue({
      name: 'orders',
      schema: identity,
      decode: (message) => message.Body,
    });

    expect(queue.consumer({ queueUrl: 'url' } as never).deserializer?.({ Body: 'raw' } as never)).toBe('raw');
  });

  it('producer() prefills the name', () => {
    const queue = defineQueue({ name: 'orders', schema: identity });
    expect(queue.producer({ queueUrl: 'url' } as never).name).toBe('orders');
  });
});
