# Changelog

## 4.0.0-alpha.1 — Bug fixes (unreleased)

Phase 1 of the v4 evolution: low-risk correctness fixes, each covered by a test.

### Fixed

- **B1** — event handlers are now bound to the provider instance that declares
  them, instead of the message handler's provider. Previously, an
  `@SqsConsumerEventHandler` living in a different class than its
  `@SqsMessageHandler` would run with the wrong `this`.
- **B3** — `SqsModule.register()` now forwards the DI-injected `SQS_OPTIONS`
  into `SqsService` instead of capturing the options from a closure, so the
  options provider is actually meaningful (removes the stray `biome-ignore`).
- **#80** — `SqsModuleAsyncOptions` now accepts `providers`, which are added to
  the module and made available to the `useFactory`/`useClass` provider. No more
  wrapper modules just to inject a `Logger` into the async factory.
- **B4** — replaced the non-null assertions in `getProducerQueueSize`/`send`
  with an explicit get-and-guard, removing the `noNonNullAssertion` warnings.

### Tests

- New `test/sqs.module.spec.ts` covers register/registerAsync DI (B3, #80).
- Flipped the B1 `it.fails` spec to a passing regression guard.

## 4.0.0-alpha.0 — Foundation (unreleased)

This is the first step of the v4 "releitura": it modernizes the tooling and
build foundation **without changing the public API yet**. Behaviour is
preserved and verified by the existing e2e suite plus new unit tests.

### Tooling & build

- **Strict TypeScript**: enabled `strict` and `verbatimModuleSyntax`; all
  type-only imports are now explicit `import type`.
- **Dual ESM + CJS build** via `tsup`, replacing the single CJS `tsc` build.
  The package now ships a proper `exports` map, `module`, `sideEffects: false`,
  and an explicit `files` allow-list. Decorator metadata is emitted through the
  `@swc/core` plugin so NestJS reflection keeps working in the built output.
- **Metadata-free internal DI**: `SqsService` now injects `DiscoveryService`
  with an explicit `@Inject(...)`, so dependency injection no longer relies on
  emitted `design:paramtypes` metadata. This is what unblocks fast, bundler-based
  builds.
- **Scripts**: added `typecheck`, `lint`, `lint:fix`, `format`, `test` (unit),
  `test:e2e`, and an aggregate `check`.
- **Package metadata**: filled in `description`, `keywords`, and bumped to
  `4.0.0-alpha.0`.

### Tests

- Split Vitest into a **unit** config (`vitest.config.ts`, broker-free, mocks
  `sqs-consumer`/`sqs-producer`) and an **e2e** config (`vitest.e2e.config.ts`,
  runs against ElasticMQ).
- Added unit coverage for consumer/producer wiring, batch handler selection,
  duplicate-name guards, message serialization, and error paths.

### Known issues tracked for later phases

- **B1** — event handlers are bound to the message handler's provider instance
  instead of their own (captured as an `it.fails` regression spec).
- **#108** — handlers are bound at `onModuleInit`, breaking post-boot method
  wrapping (tracing/metrics/transactional decorators).
- **#80** — `providers` missing from `SqsModuleAsyncOptions`.
- **#89** — no global/catch-all error handler across consumers.
- Dependency majors pending validation: `sqs-consumer` 15, `sqs-producer` 9,
  `@golevelup/nestjs-discovery` 7, `biome` 2, `vitest` 4 (fixes #103/#99/#101).
