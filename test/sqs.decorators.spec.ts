import 'reflect-metadata';
import { describe, expect, it } from 'vitest';
import { SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD } from '../lib/sqs.constants';
import { SqsConsumerEventHandler, SqsMessageHandler } from '../lib/sqs.decorators';

// Apply a method decorator to a throwaway method and read back the metadata that
// SetMetadata stored on the method function.
function metaOf(decorator: MethodDecorator, key: symbol): unknown {
  class Dummy {
    handle() {}
  }
  const descriptor = Object.getOwnPropertyDescriptor(Dummy.prototype, 'handle');
  decorator(Dummy.prototype, 'handle', descriptor as PropertyDescriptor);
  return Reflect.getMetadata(key, Dummy.prototype.handle);
}

describe('SqsMessageHandler', () => {
  it('accepts the positional form', () => {
    expect(metaOf(SqsMessageHandler('q', true), SQS_CONSUMER_METHOD)).toEqual({ name: 'q', batch: true });
  });

  it('accepts the options-object form', () => {
    expect(metaOf(SqsMessageHandler({ name: 'q', batch: true }), SQS_CONSUMER_METHOD)).toEqual({
      name: 'q',
      batch: true,
    });
  });

  it('defaults batch to undefined in the positional form', () => {
    expect(metaOf(SqsMessageHandler('q'), SQS_CONSUMER_METHOD)).toEqual({ name: 'q', batch: undefined });
  });
});

describe('SqsConsumerEventHandler', () => {
  it('accepts the positional form', () => {
    expect(metaOf(SqsConsumerEventHandler('q', 'processing_error'), SQS_CONSUMER_EVENT_HANDLER)).toEqual({
      name: 'q',
      eventName: 'processing_error',
    });
  });

  it('accepts the options-object form', () => {
    expect(
      metaOf(SqsConsumerEventHandler({ name: 'q', eventName: 'processing_error' }), SQS_CONSUMER_EVENT_HANDLER),
    ).toEqual({ name: 'q', eventName: 'processing_error' });
  });
});
