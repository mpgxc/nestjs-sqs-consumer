export const SQS_OPTIONS = Symbol.for('SQS_OPTIONS');

export const SQS_CONSUMER_METHOD = Symbol.for('SQS_CONSUMER_METHOD');
export const SQS_CONSUMER_EVENT_HANDLER = Symbol.for('SQS_CONSUMER_EVENT_HANDLER');

/**
 * Wildcard consumer name. An `@SqsConsumerEventHandler(ALL_CONSUMERS, event)`
 * is attached to every registered consumer, giving a single catch-all handler
 * for an event (e.g. `processing_error`) across all queues. (#89)
 */
export const ALL_CONSUMERS = '*';
