import { DatabaseSync } from 'node:sqlite';
import { describe, expect, it } from 'vitest';
import type { IMessageBroker, MessageTransaction } from '@typekafka/broker';
import { OutboxRelay, OutboxStore } from '../src/outbox.js';
import { createLogger } from '../src/logger.js';

const logger = createLogger('silent');

describe('OutboxRelay transactional failure recovery', () => {
  it('retries the full batch after an intermediate produce failure aborts the transaction', async () => {
    const db = new DatabaseSync(':memory:');
    try {
      const store = new OutboxStore(db);
      store.insertPending({ topic: 'orders.created', payload: { n: 1 } });
      store.insertPending({ topic: 'payments.completed', payload: { n: 2 } });

      const producedByAttempt: string[][] = [];
      let aborts = 0;
      let commits = 0;
      const broker = {
        beginTransaction: async (): Promise<MessageTransaction> => {
          const attempt = producedByAttempt.length;
          producedByAttempt.push([]);
          return {
            produce: async (topic: string) => {
              producedByAttempt[attempt]!.push(topic);
              if (attempt === 0 && topic === 'payments.completed') {
                throw new Error('second produce failed');
              }
              return { topic, partition: 0, offset: '0' };
            },
            commit: async () => { commits++; },
            abort: async () => { aborts++; },
          };
        },
      } as IMessageBroker;
      const relay = new OutboxRelay({ broker, store, logger });

      expect(await relay.runOnce()).toBe(0);
      expect(store.countPending()).toBe(2);
      expect(aborts).toBe(1);
      expect(commits).toBe(0);

      expect(await relay.runOnce()).toBe(2);
      expect(producedByAttempt).toEqual([
        ['orders.created', 'payments.completed'],
        ['orders.created', 'payments.completed'],
      ]);
      expect(commits).toBe(1);
      expect(store.countPending()).toBe(0);
    } finally {
      db.close();
    }
  });

  it('retries pending rows when transaction commit fails before acknowledgement', async () => {
    const db = new DatabaseSync(':memory:');
    try {
      const store = new OutboxStore(db);
      store.insertPending({ topic: 'orders.created', payload: { n: 1 } });

      let produced = 0;
      let commits = 0;
      let aborts = 0;
      const broker = {
        beginTransaction: async (): Promise<MessageTransaction> => ({
          produce: async (topic: string) => {
            produced++;
            return { topic, partition: 0, offset: String(produced) };
          },
          commit: async () => {
            commits++;
            if (commits === 1) throw new Error('transaction commit failed');
          },
          abort: async () => { aborts++; },
        }),
      } as IMessageBroker;
      const relay = new OutboxRelay({ broker, store, logger });

      expect(await relay.runOnce()).toBe(0);
      expect(store.countPending()).toBe(1);
      expect(aborts).toBe(1);

      expect(await relay.runOnce()).toBe(1);
      expect(produced).toBe(2);
      expect(commits).toBe(2);
      expect(store.countPending()).toBe(0);
    } finally {
      db.close();
    }
  });
});
