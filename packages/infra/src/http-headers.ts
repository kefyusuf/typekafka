import type { ServerResponse } from 'node:http';
import type { RequestHandler } from 'express';

const SECURITY_HEADERS: ReadonlyArray<readonly [string, string]> = [
  ['X-Content-Type-Options', 'nosniff'],
  ['X-Frame-Options', 'DENY'],
  ['Referrer-Policy', 'no-referrer'],
];

/**
 * Minimal dependency-free hardening for the demo HTTP surfaces (a small
 * helmet substitute). Rate limiting is intentionally out of scope: the
 * threat model is local development, not internet exposure.
 */
export function applySecurityHeaders(res: ServerResponse): void {
  for (const [name, value] of SECURITY_HEADERS) {
    if (!res.hasHeader(name)) res.setHeader(name, value);
  }
}

export function securityHeadersMiddleware(): RequestHandler {
  return (_req, res, next) => {
    applySecurityHeaders(res);
    next();
  };
}