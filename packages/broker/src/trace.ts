import { SpanStatusCode, trace } from '@opentelemetry/api';
import type { Attributes, Span } from '@opentelemetry/api';

const tracer = trace.getTracer('@nodejs-kafka/broker');

/**
 * Runs `fn` inside a child span of the current active context. When no OTel
 * SDK is registered the global provider is a no-op, so callers outside of a
 * tracing setup observe zero overhead.
 */
export async function withSpan<T>(
  name: string,
  attributes: Attributes,
  fn: () => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(name, async (span: Span) => {
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
  });
}
