import { describe, expect, it } from 'vitest';
import type { Request, Response } from 'express';
import { securityHeadersMiddleware } from '../src/http-headers.js';
import { createMetrics, startMetricsServer } from '../src/metrics.js';

function fakeRes() {
  const headers = new Map<string, string>();
  return {
    headers,
    setHeader(name: string, value: string) {
      headers.set(name.toLowerCase(), value);
    },
    hasHeader(name: string) {
      return headers.has(name.toLowerCase());
    },
  };
}

describe('securityHeadersMiddleware', () => {
  it('sets nosniff, frame-options and referrer-policy, then calls next', () => {
    const res = fakeRes();
    let nextCalled = 0;

    securityHeadersMiddleware()(
      {} as Request,
      res as unknown as Response,
      () => {
        nextCalled += 1;
      },
    );

    expect(res.headers.get('x-content-type-options')).toBe('nosniff');
    expect(res.headers.get('x-frame-options')).toBe('DENY');
    expect(res.headers.get('referrer-policy')).toBe('no-referrer');
    expect(nextCalled).toBe(1);
  });

  it('does not overwrite headers already set by an earlier handler', () => {
    const res = fakeRes();
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');

    securityHeadersMiddleware()({} as Request, res as unknown as Response, () => {});

    expect(res.headers.get('x-frame-options')).toBe('SAMEORIGIN');
    expect(res.headers.get('x-content-type-options')).toBe('nosniff');
  });
});

describe('startMetricsServer security headers', () => {
  it('serves /metrics with the security headers applied', async () => {
    const metrics = createMetrics();
    const server = await startMetricsServer(0, metrics.registry);
    try {
      const res = await fetch(`http://127.0.0.1:${server.port}/metrics`);
      expect(res.status).toBe(200);
      expect(res.headers.get('x-content-type-options')).toBe('nosniff');
      expect(res.headers.get('x-frame-options')).toBe('DENY');
      expect(res.headers.get('referrer-policy')).toBe('no-referrer');
    } finally {
      await server.close();
    }
  });

  it('applies the security headers to error responses too', async () => {
    const metrics = createMetrics();
    const server = await startMetricsServer(0, metrics.registry);
    try {
      const res = await fetch(`http://127.0.0.1:${server.port}/nope`);
      expect(res.status).toBe(404);
      expect(res.headers.get('x-content-type-options')).toBe('nosniff');
    } finally {
      await server.close();
    }
  });
});