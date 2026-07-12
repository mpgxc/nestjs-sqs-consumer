import { Test } from '@nestjs/testing';
import { describe, expect, it, vi } from 'vitest';

// Mock the underlying bbc libraries so module init never touches the network.
vi.mock('sqs-consumer', () => ({
  Consumer: { create: vi.fn(() => ({ start: vi.fn(), stop: vi.fn(), addListener: vi.fn() })) },
}));
vi.mock('sqs-producer', () => ({
  Producer: { create: vi.fn(() => ({ send: vi.fn(), queueSize: vi.fn() })) },
}));

const { SqsModule } = await import('../lib/sqs.module');
const { SqsService } = await import('../lib/sqs.service');

describe('SqsModule — dependency injection', () => {
  it('resolves SqsService from register() with the provided options (B3)', async () => {
    const options = { consumers: [], producers: [] };
    const moduleRef = await Test.createTestingModule({
      imports: [SqsModule.register(options)],
    }).compile();
    await moduleRef.init();

    const service = moduleRef.get(SqsService);
    // The factory must forward the *injected* options, not a stale closure.
    expect(service.options).toBe(options);

    await moduleRef.close();
  });

  it('injects providers passed through registerAsync() into the factory (#80)', async () => {
    const TOKEN = 'CUSTOM_DEP';

    const moduleRef = await Test.createTestingModule({
      imports: [
        SqsModule.registerAsync({
          providers: [{ provide: TOKEN, useValue: { tag: 'from-provider' } }],
          useFactory: (dep: { tag: string }) => {
            // The custom provider must be resolvable here — this throws if #80
            // regressed and `providers` were dropped from the module metadata.
            expect(dep.tag).toBe('from-provider');
            return { consumers: [], producers: [] };
          },
          inject: [TOKEN],
        }),
      ],
    }).compile();
    await moduleRef.init();

    expect(moduleRef.get(SqsService)).toBeInstanceOf(SqsService);
    await moduleRef.close();
  });
});
