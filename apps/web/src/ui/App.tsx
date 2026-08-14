import { useEffect, useState } from 'react';
import type { TelemetryEvent, TelemetryEventType } from '@nodejs-kafka/domain';
import { FlowDiagram } from './FlowDiagram.js';
import { EventLog } from './EventLog.js';
import { OrderForm } from './OrderForm.js';

const MAX_EVENTS = 200;

export function App() {
  const [events, setEvents] = useState<TelemetryEvent[]>([]);
  const [active, setActive] = useState<TelemetryEventType | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastOrder, setLastOrder] = useState<string | null>(null);

  useEffect(() => {
    const source = new EventSource('/api/events');
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (event) => {
      try {
        const telemetry = JSON.parse(event.data) as TelemetryEvent;
        setActive(telemetry.type);
        setEvents((prev) => [...prev.slice(-(MAX_EVENTS - 1)), telemetry]);
      } catch {
        // ignore malformed frames
      }
    };
    return () => source.close();
  }, []);

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', padding: 24, maxWidth: 1100, margin: '0 auto' }}>
      <h1>Node.js Kafka — live flow</h1>
      <p>
        Place an order, then watch it travel: <code>produce → orders.created →
        consume → retry → DLQ → commit</code>.
      </p>
      <div style={{ margin: '12px 0' }}>
        <span
          style={{
            display: 'inline-block',
            width: 10,
            height: 10,
            borderRadius: 5,
            background: connected ? '#4caf50' : '#f44336',
            marginRight: 6,
          }}
        />
        {connected ? 'SSE connected' : 'SSE disconnected'}
      </div>
      <FlowDiagram active={active} />
      <OrderForm onPlaced={(orderId, oversized) => {
        setLastOrder(orderId);
        if (oversized) setActive('retrying');
      }} />
      {lastOrder && <p>Order <code>{lastOrder}</code> placed.</p>}
      <EventLog events={events} />
    </div>
  );
}
