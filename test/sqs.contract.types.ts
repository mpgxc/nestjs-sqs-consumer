/**
 * Type-level checks for the typed queue contract. This file is compiled by
 * `tsc` (via `pnpm run typecheck`) but is not a runtime spec — vitest only picks
 * up `*.spec.ts`. If end-to-end inference regresses, typecheck fails here.
 */

import type { Payload } from '../lib/sqs.contract';
import { defineQueue } from '../lib/sqs.contract';
import type { SqsService } from '../lib/sqs.service';

interface Order {
  id: string;
  total: number;
}

const OrderSchema = { parse: (_input: unknown): Order => ({ id: '', total: 0 }) };
const OrdersQueue = defineQueue({ name: 'orders', schema: OrderSchema });

// `Payload<Q>` resolves to the schema's output type.
const good: Payload<typeof OrdersQueue> = { id: 'x', total: 1 };
// @ts-expect-error — `total` is required by the schema.
const bad: Payload<typeof OrdersQueue> = { id: 'x' };

declare const sqs: SqsService;

// `send(queue, ...)` type-checks the body against the queue's schema.
void sqs.send(OrdersQueue, { id: '1', body: { id: 'x', total: 2 } });
// @ts-expect-error — body is missing `total`.
void sqs.send(OrdersQueue, { id: '1', body: { id: 'x' } });

void good;
void bad;
