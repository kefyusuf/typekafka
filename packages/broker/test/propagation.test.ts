import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { context, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { extractParentContext, injectTraceContext } from '../src/trace.js';

describe('trace context propagation', () => {
  let provider: BasicTracerProvider;
  let exporter: InMemorySpanExporter;

  beforeAll(() => {
    // Production's NodeSDK installs a real (AsyncLocalStorage) context manager,
    // which is what makes `trace.getActiveSpan()` work inside a span. Install
    // the equivalent so the producer->consumer linking can be exercised here.
    context.setGlobalContextManager(new AsyncLocalStorageContextManager());
    exporter = new InMemorySpanExporter();
    provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });
    trace.setGlobalTracerProvider(provider);
  });

  afterAll(async () => {
    await provider.forceFlush();
    await provider.shutdown();
    trace.disable();
    context.disable();
  });

  it('injects a well-formed traceparent and reconstructs the parent context', () => {
    const tracer = trace.getTracer('test');
    tracer.startActiveSpan('parent', (span) => {
      const sc = span.spanContext();
      const headers = injectTraceContext({ foo: 'bar' });
      expect(headers?.foo).toBe('bar');
      const tp = (
        Array.isArray(headers?.traceparent)
          ? headers?.traceparent[0]
          : headers?.traceparent
      ) as string;
      expect(tp).toBe(
        `00-${sc.traceId}-${sc.spanId}-${sc.traceFlags.toString(16).padStart(2, '0')}`,
      );

      const parentCtx = extractParentContext(headers!);
      expect(parentCtx).toBeDefined();
      const restored = trace.getSpanContext(parentCtx!);
      // The reconstructed context is the *parent* (the producer's span): same
      // trace, same spanId, but flagged as remote — so the consumer span links
      // back across the Kafka boundary.
      expect(restored?.traceId).toBe(sc.traceId);
      expect(restored?.spanId).toBe(sc.spanId);
      expect(restored?.isRemote).toBe(true);
      span.end();
    });
  });

  it('is a no-op without an active span and rejects malformed input', () => {
    expect(injectTraceContext({ a: '1' })).toEqual({ a: '1' });
    expect(extractParentContext({})).toBeUndefined();
    expect(extractParentContext({ traceparent: 'garbage' })).toBeUndefined();
  });
});
