import type React from 'react';
import type { TelemetryEventType } from '@nodejs-kafka/domain';

interface Props {
  activeSteps: TelemetryEventType[];
  current: TelemetryEventType | null;
}

type StageState = 'current' | 'done' | 'idle';
type StageKey = 'produce' | 'consume' | 'payment' | 'dlq';

// Which telemetry event types light up each node, and which "stage" number they
// map to so the journey can be labelled 1, 2, 3, 4 ... as it progresses.
const KINDS: Record<StageKey, TelemetryEventType[]> = {
  produce: ['produced'],
  consume: ['consumed', 'parsed', 'committed', 'retrying'],
  payment: ['payment-recorded'],
  dlq: ['dead-lettered', 'invalid-to-dlq'],
};

function eventStage(t: TelemetryEventType): StageKey | null {
  if (t === 'produced') return 'produce';
  if (KINDS.consume.includes(t)) return 'consume';
  if (KINDS.payment.includes(t)) return 'payment';
  if (KINDS.dlq.includes(t)) return 'dlq';
  return null;
}

// Assign 1-based step numbers to stages in the order they first activate.
function stageNumbers(activeSteps: TelemetryEventType[]): Partial<Record<StageKey, number>> {
  const out: Partial<Record<StageKey, number>> = {};
  let n = 0;
  for (const t of activeSteps) {
    const s = eventStage(t);
    if (s && out[s] === undefined) {
      n += 1;
      out[s] = n;
    }
  }
  return out;
}

function stageState(
  activeSteps: TelemetryEventType[],
  current: TelemetryEventType | null,
  kinds: TelemetryEventType[],
): StageState {
  if (current && kinds.includes(current)) return 'current';
  if (kinds.some((k) => activeSteps.includes(k))) return 'done';
  return 'idle';
}

const idleBox: React.CSSProperties = {
  border: '1px solid #ddd',
  borderRadius: 6,
  padding: '8px 12px',
  background: '#fafafa',
  fontSize: 14,
  color: '#aaa',
  transition: 'all 300ms',
  display: 'flex',
  alignItems: 'center',
  gap: 8,
};

const idleEdge: React.CSSProperties = {
  color: '#ccc',
  fontWeight: 400,
  transition: 'all 300ms',
};

function boxStyle(state: StageState, accent: string): React.CSSProperties {
  if (state === 'idle') return idleBox;
  return {
    border: `2px solid ${accent}`,
    borderRadius: 6,
    padding: '7px 12px',
    fontSize: 14,
    background: '#fff',
    color: accent,
    fontWeight: state === 'current' ? 700 : 600,
    ['--pulse' as string]: `${accent}73`,
    animation: state === 'current' ? 'flowPulse 1.2s ease-in-out infinite' : 'none',
    transition: 'all 300ms',
    display: 'flex',
    alignItems: 'center',
    gap: 8,
  } as React.CSSProperties;
}

function edgeStyle(state: StageState, accent: string): React.CSSProperties {
  if (state === 'idle') return idleEdge;
  return { color: accent, fontWeight: state === 'current' ? 700 : 600, transition: 'all 300ms' };
}

const badgeStyle = (accent: string): React.CSSProperties => ({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  width: 20,
  height: 20,
  borderRadius: '50%',
  background: accent,
  color: '#fff',
  fontSize: 12,
  fontWeight: 700,
  flexShrink: 0,
});

const PULSE_KEYFRAMES = `@keyframes flowPulse {
  0% { box-shadow: 0 0 0 0 var(--pulse, rgba(230,81,0,.45)); }
  70% { box-shadow: 0 0 0 9px transparent; }
  100% { box-shadow: 0 0 0 0 transparent; }
}`;

interface NodeProps {
  label: string;
  accent: string;
  state: StageState;
  step?: number;
}

function Node({ label, accent, state, step }: NodeProps) {
  return (
    <div style={boxStyle(state, accent)}>
      {step !== undefined && <span style={badgeStyle(accent)}>{step}</span>}
      <span>{label}</span>
    </div>
  );
}

export function FlowDiagram({ activeSteps, current }: Props) {
  const s = (k: TelemetryEventType[]) => stageState(activeSteps, current, k);
  const nums = stageNumbers(activeSteps);
  return (
    <div style={{ border: '1px solid #ddd', borderRadius: 8, padding: 16, margin: '16px 0', background: '#fff' }}>
      <style>{PULSE_KEYFRAMES}</style>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <Node label="Producer" accent="#e65100" state={s(KINDS.produce)} step={nums.produce} />
        <div style={edgeStyle(s(KINDS.produce), '#e65100')}>→ produce</div>
        <Node label="orders.created" accent="#1976d2" state={s(KINDS.produce)} step={nums.produce} />
        <div style={edgeStyle(s(KINDS.consume), '#2e7d32')}>→ consume</div>
        <Node label="Consumer" accent="#2e7d32" state={s(KINDS.consume)} step={nums.consume} />
      </div>
      <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap', paddingLeft: 4 }}>
        <span style={{ color: '#888', fontSize: 13 }}>Consumer branch:</span>
        <div style={edgeStyle(s(KINDS.payment), '#6a1b9a')}>→ payments completed</div>
        <Node label="payments.completed" accent="#6a1b9a" state={s(KINDS.payment)} step={nums.payment} />
        <div style={edgeStyle(s(KINDS.payment), '#6a1b9a')}>→ payment recorded</div>
      </div>
      <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap', paddingLeft: 4 }}>
        <span style={{ color: '#888', fontSize: 13 }}>on oversized:</span>
        <div style={edgeStyle(s(KINDS.dlq), '#c62828')}>→ DLQ</div>
        <Node label="orders.dlq" accent="#c62828" state={s(KINDS.dlq)} step={nums.dlq} />
      </div>
    </div>
  );
}
