/**
 * Orders whose total exceeds this threshold (in cents) are treated as
 * "oversized" by the sample/notification pipeline — e.g. the notification
 * handler simulates a provider timeout for them to exercise retry + DLQ.
 */
export const OVERSIZED_THRESHOLD_CENTS = 100_000;
