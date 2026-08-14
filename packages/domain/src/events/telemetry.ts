import { z } from 'zod';

export const TELEMETRY_TOPIC = 'telemetry.events';

export const TelemetryEventTypeSchema = z.enum([
  'produced',
  'consumed',
  'parsed',
  'retrying',
  'dead-lettered',
  'committed',
  'payment-recorded',
  'invalid-to-dlq',
]);

export type TelemetryEventType = z.infer<typeof TelemetryEventTypeSchema>;

export const TelemetryEventSchema = z.object({
  type: TelemetryEventTypeSchema,
  topic: z.string().min(1),
  eventId: z.string().min(1),
  orderId: z.string().min(1),
  partition: z.number().int().nonnegative(),
  offset: z.string().min(1),
  attempt: z.number().int().positive().optional(),
  message: z.string().min(1),
  concept: z.string().min(1),
  occurredAt: z.string().datetime(),
});

export type TelemetryEvent = z.infer<typeof TelemetryEventSchema>;
