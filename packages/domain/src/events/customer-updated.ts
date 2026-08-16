import { z } from 'zod';

export const CustomerUpdatedSchema = z.object({
  type: z.literal('customer.updated'),
  eventId: z.string().uuid(),
  occurredAt: z.string().datetime(),
  customerId: z.string().min(1),
  totalSpentCents: z.number().int().nonnegative(),
  orderCount: z.number().int().nonnegative(),
  lastOrderAt: z.string().datetime(),
  updatedAt: z.string().datetime(),
});

export type CustomerUpdated = z.infer<typeof CustomerUpdatedSchema>;
