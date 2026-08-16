import type { ProduceResult } from '@nodejs-kafka/broker';
import type { AppMetrics, OutboxRow, TelemetryClient } from '@nodejs-kafka/infra';

export function makeProducedHook(
  telemetry: TelemetryClient,
  metrics?: AppMetrics,
): (published: { row: OutboxRow; result: ProduceResult }) => Promise<void> {
  return async (published) => {
    metrics?.outboxPublishedTotal.labels({ topic: published.row.topic }).inc();
    const payload = published.row.payload as { eventId?: string; orderId?: string };
    await telemetry.emit({
      type: 'produced',
      topic: published.row.topic,
      eventId: payload.eventId ?? '',
      orderId: payload.orderId ?? '',
      partition: published.result.partition,
      offset: published.result.offset,
      message: `Event published to ${published.row.topic} via outbox relay`,
      concept: 'transactional-outbox',
    });
  };
}
