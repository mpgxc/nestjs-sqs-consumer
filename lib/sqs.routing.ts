import type { Message as SqsRawMessage } from '@aws-sdk/client-sqs';

/**
 * Extracts a routing key from a raw SQS message, so it can be dispatched to the
 * `@SqsMessageHandler({ name, type })` whose `type` matches. Return `undefined`
 * to fall through to the untyped fallback handler (or trigger `onUnmatched`).
 */
export type Discriminator = (message: SqsRawMessage) => string | undefined;

/**
 * Discriminate on a field of the JSON message body, e.g. `byBodyField('status')`
 * routes on `JSON.parse(message.Body).status`. Returns `undefined` when the body
 * is absent, not valid JSON, or the field is null/undefined.
 */
export function byBodyField(field: string): Discriminator {
  return (message) => {
    if (!message.Body) {
      return undefined;
    }
    try {
      const value = (JSON.parse(message.Body) as Record<string, unknown>)?.[field];
      return value == null ? undefined : String(value);
    } catch {
      return undefined;
    }
  };
}

/**
 * Discriminate on an SQS message attribute's `StringValue`, e.g.
 * `byMessageAttribute('eventType')`.
 */
export function byMessageAttribute(name: string): Discriminator {
  return (message) => message.MessageAttributes?.[name]?.StringValue ?? undefined;
}
