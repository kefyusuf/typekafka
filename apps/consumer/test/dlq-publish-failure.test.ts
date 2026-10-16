import { describe, expect, it, vi } from 'vitest';
import type { ConsumeContext, IMessageBroker, KafkaMessage } from '@typekafka/broker';
import { DlqManager, RetryTopicScheduler, type AppLogger } from '@typekafka/infra';
import { createHandlerRunner } from '../src/handler-runner.js';
import { createRetryTopicRunner } from '../src/retry-runner.js';

const logger = {
  warn: vi.fn(),
  error: vi.fn(),
} as unknown as AppLogger;

const sourceMessage: KafkaMessage<unknown> = {
  topic: 'orders.created',
  key: 'ORD-00042',
  value: { orderId: 'ORD-00042' },
  partition: 0,
  offset: '42',
  timestamp: '2026-10-16T00:00:00.000Z',
};

function failingDlq() {
  const deadLetter = vi.fn().mockRejectedValue(new Error('DLQ produce failed'));
  return { deadLetter } as unknown as DlqManager;
}

function commitContext() {
  const commit = vi.fn(async () => {});
  return { commit } as ConsumeContext;
}

describe('DLQ publish failure must not acknowledge the source offset', () => {
  it.each(['invalid payload', 'exhausted handler'] as const)(
    'source runner: %s',
    async (failure) => {
      const dlq = failingDlq();
      const context = commitContext();
      const handler = vi.fn(async () => {
        throw new Error('downstream failed');
      });
      const runner = createHandlerRunner(
        {} as IMessageBroker,
        dlq,
        {
          parse: (value: unknown) => {
            if (failure === 'invalid payload') throw new Error('invalid payload');
            return value;
          },
          handler,
          attempts: 1,
          baseDelayMs: 1,
        },
        logger,
      );

      await expect(runner(sourceMessage, context)).rejects.toThrow('DLQ produce failed');
      expect(dlq.deadLetter).toHaveBeenCalledOnce();
      expect(context.commit).not.toHaveBeenCalled();
      if (failure === 'invalid payload') expect(handler).not.toHaveBeenCalled();
    },
  );

  it.each(['invalid payload', 'exhausted handler'] as const)(
    'retry-topic runner: %s',
    async (failure) => {
      const dlq = failingDlq();
      const context = commitContext();
      const scheduler = new RetryTopicScheduler({} as IMessageBroker);
      const handler = vi.fn(async () => {
        throw new Error('downstream failed');
      });
      const runner = createRetryTopicRunner(
        scheduler,
        dlq,
        {
          parse: (value: unknown) => {
            if (failure === 'invalid payload') throw new Error('invalid payload');
            return value;
          },
          handler,
          attempts: 1,
          baseDelayMs: 1,
        },
        logger,
      );
      const message: KafkaMessage<unknown> = {
        ...sourceMessage,
        topic: scheduler.topicName,
        headers: {
          'retry.original-topic': sourceMessage.topic,
          'retry-count': String(scheduler.maxDeliveries),
          'next-deliver-at': '2026-01-01T00:00:00.000Z',
        },
      };

      await expect(runner(message, context)).rejects.toThrow('DLQ produce failed');
      expect(dlq.deadLetter).toHaveBeenCalledOnce();
      expect(context.commit).not.toHaveBeenCalled();
      if (failure === 'invalid payload') expect(handler).not.toHaveBeenCalled();
    },
  );
});
