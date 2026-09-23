import { Fragment } from 'react';
import type { TelemetryEvent, TelemetryEventType } from '@typekafka/domain';

interface Props {
  events: TelemetryEvent[];
}

const TYPE_COLORS: Record<TelemetryEventType, string> = {
  produced: '#4caf50',
  consumed: '#2196f3',
  parsed: '#9c27b0',
  retrying: '#ff9800',
  'retry-parked': '#ffc107',
  'retry-scheduled': '#ffb74d',
  'dead-lettered': '#f44336',
  committed: '#009688',
  'payment-recorded': '#3f51b5',
  'invalid-to-dlq': '#e91e63',
  'duplicate-skipped': '#607d8b',
};

const CONCEPT_EXPLANATION: Record<string, string> = {
  'partition-key': 'The orderId is hashed to pick one of the topic\'s partitions.',
  'consumer-group': 'A consumer group balances partitions across its members.',
  'schema-validation': 'Every payload is validated against a Zod schema before handling.',
  retry: 'Transient failures back off exponentially with jitter, then retry.',
  'dead-letter-queue': 'Messages that exhaust retries are parked in orders.dlq.',
  'offset-commit': 'The offset is committed only after the message was handled.',
};

export function EventLog({ events }: Props) {
  return (
    <div style={{ marginTop: 24 }}>
      <h2>Live event log</h2>
      {events.length === 0 && <p>No events yet — place an order.</p>}
      <ul style={{ listStyle: 'none', padding: 0 }}>
        {events.map((event, i) => {
          const isNewRequest = i === 0 || events[i - 1]?.orderId !== event.orderId;
          return (
            <Fragment key={`${event.eventId}-${i}`}>
              {isNewRequest && (
                <li style={{ marginTop: i === 0 ? 0 : 12 }}>
                  <hr
                    style={{
                      border: 'none',
                      borderTop: '1px dashed #ccc',
                      margin: '0 0 12px',
                    }}
                  />
                  <span style={{ color: '#666', fontSize: 12, textTransform: 'uppercase', letterSpacing: 1 }}>
                    Order {event.orderId}
                  </span>
                </li>
              )}
              <li
                style={{ display: 'flex', gap: 12, padding: '8px 0', borderBottom: '1px solid #eee' }}
              >
                <span
                  style={{
                    background: TYPE_COLORS[event.type],
                    color: '#fff',
                    borderRadius: 4,
                    padding: '2px 8px',
                    fontSize: 12,
                    whiteSpace: 'nowrap',
                  }}
                >
                  {event.type}
                </span>
                <span style={{ fontFamily: 'monospace', fontSize: 13 }}>
                  {event.topic}#{event.partition}@{event.offset}
                </span>
                <span style={{ flex: 1 }}>{event.message}</span>
                <span title={CONCEPT_EXPLANATION[event.concept] ?? ''} style={{ color: '#666', fontSize: 13 }}>
                  {event.concept}
                </span>
              </li>
            </Fragment>
          );
        })}
      </ul>
    </div>
  );
}
