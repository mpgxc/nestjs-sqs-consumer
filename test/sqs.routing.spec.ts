import { describe, expect, it } from 'vitest';
import { byBodyField, byMessageAttribute } from '../lib/sqs.routing';

describe('byBodyField', () => {
  const discriminate = byBodyField('status');

  it('reads the field from a JSON body', () => {
    expect(discriminate({ Body: '{"status":"approved"}' } as never)).toBe('approved');
  });

  it('coerces non-string values to strings', () => {
    expect(discriminate({ Body: '{"status":42}' } as never)).toBe('42');
  });

  it('returns undefined for a missing field, missing body, or invalid JSON', () => {
    expect(discriminate({ Body: '{"other":1}' } as never)).toBeUndefined();
    expect(discriminate({} as never)).toBeUndefined();
    expect(discriminate({ Body: 'not-json' } as never)).toBeUndefined();
    expect(discriminate({ Body: '{"status":null}' } as never)).toBeUndefined();
  });
});

describe('byMessageAttribute', () => {
  const discriminate = byMessageAttribute('eventType');

  it('reads the StringValue of the named attribute', () => {
    expect(discriminate({ MessageAttributes: { eventType: { StringValue: 'rejected' } } } as never)).toBe('rejected');
  });

  it('returns undefined when the attribute is absent', () => {
    expect(discriminate({ MessageAttributes: {} } as never)).toBeUndefined();
    expect(discriminate({} as never)).toBeUndefined();
  });
});
