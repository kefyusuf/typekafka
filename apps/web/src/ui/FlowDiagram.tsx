import type React from 'react';
import type { TelemetryEventType } from '@nodejs-kafka/domain';

interface Props {
  active: TelemetryEventType | null;
}

const box: React.CSSProperties = {
  border: '1px solid #444',
  borderRadius: 6,
  padding: '8px 12px',
  background: '#fafafa',
  fontSize: 14,
};

const edgeActive = (active: TelemetryEventType | null, kinds: TelemetryEventType[]): boolean =>
  active !== null && kinds.includes(active);

const edgeStyle = (active: boolean): React.CSSProperties => ({
  color: active ? '#e65100' : '#999',
  fontWeight: active ? 700 : 400,
  transition: 'all 300ms',
});

export function FlowDiagram({ active }: Props) {
  return (
    <div style={{ border: '1px solid #ddd', borderRadius: 8, padding: 16, margin: '16px 0' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <div style={box}>Producer</div>
        <div style={edgeStyle(edgeActive(active, ['produced']))}>→ produce</div>
        <div style={{ ...box, borderColor: '#1976d2' }}>orders.created</div>
        <div style={edgeStyle(edgeActive(active, ['consumed', 'parsed', 'committed', 'retrying']))}>→ consume</div>
        <div style={{ ...box, borderColor: '#2e7d32' }}>Consumer</div>
        <div style={edgeStyle(edgeActive(active, ['dead-lettered', 'invalid-to-dlq']))}>→ DLQ</div>
        <div style={{ ...box, borderColor: '#c62828' }}>orders.dlq</div>
      </div>
      <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <div style={edgeStyle(edgeActive(active, ['produced']))}>payments.completed</div>
        <div style={edgeStyle(edgeActive(active, ['payment-recorded']))}>→ payment recorded</div>
      </div>
    </div>
  );
}
