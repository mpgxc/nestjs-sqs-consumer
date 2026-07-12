import { SetMetadata } from '@nestjs/common';
import { SQS_CONSUMER_EVENT_HANDLER, SQS_CONSUMER_METHOD } from './sqs.constants';
import type { SqsConsumerEventHandlerMeta, SqsMessageHandlerMeta } from './sqs.types';

/**
 * Mark a provider method as the handler for a consumer's messages.
 *
 * Accepts either an options object (preferred) or the legacy positional form:
 *
 * ```ts
 * @SqsMessageHandler({ name: 'my-queue', batch: true })
 * @SqsMessageHandler('my-queue', true) // legacy
 * ```
 */
export function SqsMessageHandler(options: SqsMessageHandlerMeta): MethodDecorator;
export function SqsMessageHandler(name: string, batch?: boolean): MethodDecorator;
export function SqsMessageHandler(nameOrOptions: string | SqsMessageHandlerMeta, batch?: boolean): MethodDecorator {
  const meta: SqsMessageHandlerMeta =
    typeof nameOrOptions === 'string' ? { name: nameOrOptions, batch } : nameOrOptions;
  return SetMetadata(SQS_CONSUMER_METHOD, meta);
}

/**
 * Mark a provider method as the handler for a consumer event (e.g.
 * `processing_error`). Use `ALL_CONSUMERS` as the name to cover every consumer.
 *
 * ```ts
 * @SqsConsumerEventHandler({ name: 'my-queue', eventName: 'processing_error' })
 * @SqsConsumerEventHandler('my-queue', 'processing_error') // legacy
 * ```
 */
export function SqsConsumerEventHandler(options: SqsConsumerEventHandlerMeta): MethodDecorator;
export function SqsConsumerEventHandler(name: string, eventName: string): MethodDecorator;
export function SqsConsumerEventHandler(
  nameOrOptions: string | SqsConsumerEventHandlerMeta,
  eventName?: string,
): MethodDecorator {
  const meta: SqsConsumerEventHandlerMeta =
    typeof nameOrOptions === 'string' ? { name: nameOrOptions, eventName: eventName as string } : nameOrOptions;
  return SetMetadata(SQS_CONSUMER_EVENT_HANDLER, meta);
}
