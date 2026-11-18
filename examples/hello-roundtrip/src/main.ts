import { runRoundtrip } from './roundtrip.js';

const { produced, received, key } = await runRoundtrip();

console.log(`Produced ${produced.type} ${produced.orderId} (total ${produced.totalCents} cents)`);
console.log(`Consumed back with key=${key}: ${received.orderId}`);
console.log('Roundtrip OK — the event was validated against the domain schema.');