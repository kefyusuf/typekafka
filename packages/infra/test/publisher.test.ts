import { describe, expect, it, vi } from 'vitest';
import { createBroker } from '@typekafka/broker';
import type { MessageTransaction } from '@typekafka/broker';
import { createSampleOrder } from '@typekafka/domain';
import { TypedPublisher } from '../src/index.js';

describe('TypedPublisher', () => {
  it('publishes a validated event through the broker', async () => {
    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'test' },
    });
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created' }]);

    const publisher = new TypedPublisher(broker);
    const order = createSampleOrder(1);
    const result = await publisher.publish('orders.created', order);

    expect(result.topic).toBe('orders.created');

    await broker.disconnect();
  });

  it('publishes through a transaction via publishTx', async () => {
    const transaction = {
      produce: vi.fn().mockResolvedValue({ topic: 'orders.created', partition: 0, offset: '1' }),
      commit: vi.fn(),
      abort: vi.fn(),
    } satisfies MessageTransaction;

    const publisher = new TypedPublisher(
      createBroker({
        driver: 'in-memory',
        connection: { brokers: ['in-memory://'], clientId: 'test' },
      }),
    );

    const order = createSampleOrder(1);
    const result = await publisher.publishTx(transaction, 'orders.created', order);

    expect(transaction.produce).toHaveBeenCalledWith('orders.created', order, undefined);
    expect(result.offset).toBe('1');
  });
});
