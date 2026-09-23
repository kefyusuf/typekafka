import { useEffect, useRef, useState } from 'react';
import type { TelemetryEvent, TelemetryEventType } from '@typekafka/domain';
import { FlowDiagram } from './FlowDiagram.js';
import { EventLog } from './EventLog.js';
import { OrderForm } from './OrderForm.js';

const MAX_EVENTS = 200;
// Pause between revealed diagram steps so a beginner can follow the journey.
const STEP_DELAY_MS = 1300;

// Canonical pipeline order. Kafka may deliver telemetry events out of the order
// they were produced (events for one order share a partition key, but producers
// emit them in a non-logical order and retries/interleaving can reorder them).
// We always reveal steps in this logical sequence so the diagram lights up
// 1 -> 2 -> 3 ... regardless of arrival order.
const CANONICAL: TelemetryEventType[] = [
  'produced',
  'consumed',
  'parsed',
  'committed',
  'retrying',
  'retry-parked',
  'retry-scheduled',
  'dead-lettered',
  'invalid-to-dlq',
  'payment-recorded',
  'duplicate-skipped',
];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function App() {
  const [events, setEvents] = useState<TelemetryEvent[]>([]);
  // `trail` is the canonical-ordered list of stages revealed so far (diagram
  // fills in step by step and stays lit); `current` is the latest (pulses).
  const [trail, setTrail] = useState<TelemetryEventType[]>([]);
  const [current, setCurrent] = useState<TelemetryEventType | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastOrder, setLastOrder] = useState<string | null>(null);

  // Telemetry types received for the current order (deduped), the canonical
  // sequence revealed so far, and whether new arrivals are pending.
  const arrivedRef = useRef<Set<TelemetryEventType>>(new Set());
  const revealedRef = useRef<TelemetryEventType[]>([]);
  const dirtyRef = useRef(false);
  const playingRef = useRef(false);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const play = async () => {
    if (playingRef.current) return;
    playingRef.current = true;
    try {
      while (mountedRef.current) {
        const next = CANONICAL.find(
          (t) => arrivedRef.current.has(t) && !revealedRef.current.includes(t),
        );
        if (!next) {
          if (!dirtyRef.current) break;
          dirtyRef.current = false;
          await sleep(150);
          continue;
        }
        const updated = [...revealedRef.current, next];
        revealedRef.current = updated;
        setTrail(updated);
        setCurrent(next);
        await sleep(STEP_DELAY_MS);
      }
    } finally {
      playingRef.current = false;
    }
  };

  useEffect(() => {
    const source = new EventSource('/api/events');
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (event) => {
      try {
        const telemetry = JSON.parse(event.data) as TelemetryEvent;
        setEvents((prev) => [...prev.slice(-(MAX_EVENTS - 1)), telemetry]);
        arrivedRef.current.add(telemetry.type);
        dirtyRef.current = true;
        void play();
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
        Place an order, then watch it travel in logical order:{' '}
        <code>produce → consume → parse → commit → payment recorded</code>.
        Each step lights up with a short pause so the journey is easy to follow.
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
      <FlowDiagram activeSteps={trail} current={current} />
      <OrderForm
        onPlaced={(orderId, oversized) => {
          setLastOrder(orderId);
          // start a fresh, paced journey for this order
          arrivedRef.current = new Set();
          revealedRef.current = [];
          dirtyRef.current = false;
          setTrail([]);
          setCurrent(null);
          if (oversized) {
            arrivedRef.current.add('retrying');
            dirtyRef.current = true;
            void play();
          }
        }}
      />
      {lastOrder && <p>Order <code>{lastOrder}</code> placed.</p>}
      <EventLog events={events} />
    </div>
  );
}
