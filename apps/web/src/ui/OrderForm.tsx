import { useState } from 'react';

interface Props {
  onPlaced: (orderId: string, oversized: boolean) => void;
}

const SKUS = ['TSHIRT-BLACK', 'MUG-WHITE', 'HOODIE-GREY', 'STICKER-OG'];

export function OrderForm({ onPlaced }: Props) {
  const [sku, setSku] = useState('TSHIRT-BLACK');
  const [quantity, setQuantity] = useState(1);
  const [unitPriceCents, setUnitPriceCents] = useState(5000);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const randomize = () => {
    setSku(SKUS[Math.floor(Math.random() * SKUS.length)]!);
    setQuantity(1 + Math.floor(Math.random() * 4));
    setUnitPriceCents(500 + Math.floor(Math.random() * 4) * 500);
  };

  const submit = async () => {
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ sku, quantity, unitPriceCents }),
      });
      if (!res.ok) {
        throw new Error(`Server responded ${res.status}`);
      }
      const body = (await res.json()) as { orderId: string; oversized: boolean };
      onPlaced(body.orderId, body.oversized);
      if (body.oversized) {
        setError('Oversized order (> 100000 cents) — watch the retry → DLQ flow.');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to place order');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{ border: '1px solid #ddd', borderRadius: 8, padding: 16, margin: '16px 0' }}>
      <h2>Place an order</h2>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'end' }}>
        <label>
          SKU
          <select value={sku} onChange={(e) => setSku(e.target.value)} style={{ display: 'block' }}>
            {SKUS.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <label>
          Quantity
          <input
            type="number"
            min={1}
            value={quantity}
            onChange={(e) => setQuantity(Number(e.target.value))}
            style={{ display: 'block', width: 80 }}
          />
        </label>
        <label>
          Unit price (cents)
          <input
            type="number"
            min={0}
            step={100}
            value={unitPriceCents}
            onChange={(e) => setUnitPriceCents(Number(e.target.value))}
            style={{ display: 'block', width: 120 }}
          />
        </label>
        <button type="button" onClick={randomize}>Randomize</button>
        <button type="button" onClick={submit} disabled={submitting}>
          {submitting ? 'Placing…' : 'Place Order'}
        </button>
      </div>
      {error && <p style={{ color: '#c62828' }}>{error}</p>}
      <p style={{ fontSize: 13, color: '#666' }}>
        Total = quantity × unit price. A total above 100000 cents triggers the
        simulated provider timeout → retry → DLQ pipeline.
      </p>
    </div>
  );
}
