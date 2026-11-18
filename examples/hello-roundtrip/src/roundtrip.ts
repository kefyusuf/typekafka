import { createBroker } from '@typekafka/broker';
import {
  TOPIC_ORDER_CREATED,
  createSampleOrder,
  parseEvent,
  type OrderCreated,
} from '@typekafka/domain';

export interface RoundtripResult {
  produced: OrderCreated;
  received: OrderCreated;
  key: string | null;
}

interface Receipt {
  event: OrderCreated;
  key: string | null;
}

const RECEIVE_TIMEOUT_MS = 5_000;

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`No message received within ${ms}ms`)),
      ms,
    );
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export async function runRoundtrip(): Promise<RoundtripResult> {
  const broker = createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'hello-roundtrip' },
  });

  await broker.connect();
  try {
    await broker.createTopics([{ name: TOPIC_ORDER_CREATED }]);

    let resolveReceipt!: (receipt: Receipt) => void;
    let rejectReceipt!: (error: unknown) => void;
    const receipt = new Promise<Receipt>((resolve, reject) => {
      resolveReceipt = resolve;
      rejectReceipt = reject;
    });

    const stop = await broker.consume<unknown>(
      [TOPIC_ORDER_CREATED],
      async (message, ctx) => {
        try {
          const event = parseEvent(TOPIC_ORDER_CREATED, message.value);
          await ctx.commit();
          resolveReceipt({ event, key: message.key });
        } catch (error) {
          rejectReceipt(error);
        }
      },
      { groupId: 'hello-roundtrip', manualCommit: true },
    );

    try {
      const produced = createSampleOrder(1);
      await broker.produce(TOPIC_ORDER_CREATED, produced, { key: produced.orderId });
      const { event, key } = await withTimeout(receipt, RECEIVE_TIMEOUT_MS);
      return { produced, received: event, key };
    } finally {
      await stop();
    }
  } finally {
    await broker.disconnect();
  }
}