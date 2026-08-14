import { z } from 'zod';

export const OrderItemSchema = z.object({
  sku: z.string().min(1),
  quantity: z.number().int().positive(),
  priceCents: z.number().int().nonnegative(),
});

export const OrderCreatedSchema = z.object({
  type: z.literal('order.created'),
  eventId: z.string().uuid(),
  occurredAt: z.string().datetime(),
  orderId: z.string().min(1),
  customerId: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  totalCents: z.number().int().nonnegative(),
});

export type OrderCreated = z.infer<typeof OrderCreatedSchema>;
export type OrderItem = z.infer<typeof OrderItemSchema>;
