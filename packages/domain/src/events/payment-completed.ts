import { z } from 'zod';

export const PaymentCompletedSchema = z.object({
  type: z.literal('payment.completed'),
  eventId: z.string().uuid(),
  occurredAt: z.string().datetime(),
  orderId: z.string().min(1),
  paymentId: z.string().min(1),
  amountCents: z.number().int().positive(),
  method: z.enum(['card', 'bank_transfer', 'wallet']),
});

export type PaymentCompleted = z.infer<typeof PaymentCompletedSchema>;
