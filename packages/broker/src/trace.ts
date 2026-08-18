import { SpanStatusCode, context, trace } from '@opentelemetry/api';
import type {
  Attributes,
  Context,
  Span,
  SpanContext,
  TraceFlags,
} from '@opentelemetry/api';

const tracer = trace.getTracer('@nodejs-kafka/broker');

const TRACEPARENT_HEADER = 'traceparent';

/**
 * Serialize the active span context into a W3C `traceparent` header so it can
 * travel with a Kafka message and be re-linked on the consuming side.
 * Returns the headers unchanged when there is no active span (e.g. no OTel SDK
 * registered), so producers outside a tracing setup pay nothing.
 */
export function injectTraceContext(
  headers?: Record<string, string | string[]>,
): Record<string, string | string[]> | undefined {
  const span = trace.getActiveSpan();
  if (!span) return headers;
  const sc = span.spanContext();
  if (!sc.traceId || !sc.spanId) return headers;
  const traceparent = `00-${sc.traceId}-${sc.spanId}-${sc.traceFlags
    .toString(16)
    .padStart(2, '0')}`;
  return { ...(headers ?? {}), [TRACEPARENT_HEADER]: traceparent };
}

/**
 * Reverse of {@link injectTraceContext}: parse a `traceparent` header into a
 * remote span context and return it wrapped as an OTel `Context` to be used as
 * the parent of the consumer's processing span.
 */
export function extractParentContext(
  headers: Record<string, string | string[]> = {},
): Context | undefined {
  const raw = headers[TRACEPARENT_HEADER];
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (!value || typeof value !== 'string') return undefined;
  const match = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i.exec(
    value,
  );
  if (!match) return undefined;
  const traceId = match[2];
  const spanId = match[3];
  const flags = match[4];
  if (!traceId || !spanId || !flags) return undefined;
  const parentSc: SpanContext = {
    traceId,
    spanId,
    traceFlags: Number.parseInt(flags, 16) as TraceFlags,
    isRemote: true,
  };
  return trace.setSpanContext(context.active(), parentSc);
}

/**
 * Runs `fn` inside a child span of the current active context (or of
 * `parentContext`, when supplied — used to continue a trace that crossed a
 * Kafka boundary). When no OTel SDK is registered the global provider is a
 * no-op, so callers outside of a tracing setup observe zero overhead.
 */
export async function withSpan<T>(
  name: string,
  attributes: Attributes,
  fn: () => Promise<T>,
  parentContext?: Context,
): Promise<T> {
  const run = <R>(cb: () => R): R =>
    parentContext ? context.with(parentContext, cb) : cb();
  return run(() =>
    tracer.startActiveSpan(name, async (span: Span) => {
      span.setAttributes(attributes);
      try {
        const result = await fn();
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (error) {
        span.recordException(error instanceof Error ? error : new Error(String(error)));
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error;
      } finally {
        span.end();
      }
    }),
  );
}
