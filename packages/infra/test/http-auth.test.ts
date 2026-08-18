import { createServer, request as httpRequest, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createBasicAuthMiddleware } from '../src/http-auth.js';

type RawHandler = (req: IncomingMessage, res: ServerResponse, next: () => void) => void;

function startServer(credentials: string | undefined): Promise<{ url: string; close: () => Promise<void> }> {
  const mw = createBasicAuthMiddleware(credentials) as unknown as RawHandler;
  const server: Server = createServer((req, res) => {
    mw(req, res, () => {
      res.statusCode = 200;
      res.end('ok');
    });
  });
  return new Promise((resolve) => {
    server.listen(0, () => {
      const { port } = server.address() as AddressInfo;
      resolve({
        url: `http://127.0.0.1:${port}`,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

function request(
  url: string,
  authHeader?: string,
): Promise<{ status: number; wwwAuthenticate?: string }> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = httpRequest(
      {
        host: u.hostname,
        port: u.port,
        path: '/',
        headers: authHeader ? { authorization: authHeader } : {},
      },
      (res) => {
        resolve({
          status: res.statusCode ?? 0,
          wwwAuthenticate: res.headers['www-authenticate'],
        });
        res.resume();
      },
    );
    req.on('error', reject);
    req.end();
  });
}

const basic = (user: string, pass: string) =>
  `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}`;

describe('createBasicAuthMiddleware', () => {
  it('passes through when no credentials are configured', async () => {
    const s = await startServer(undefined);
    try {
      const res = await request(s.url);
      expect(res.status).toBe(200);
    } finally {
      await s.close();
    }
  });

  it('returns 401 with a WWW-Authenticate challenge when the header is missing', async () => {
    const s = await startServer('alice:secret');
    try {
      const res = await request(s.url);
      expect(res.status).toBe(401);
      expect(res.wwwAuthenticate).toContain('Basic realm="nodejs-kafka"');
    } finally {
      await s.close();
    }
  });

  it('returns 200 for a valid Authorization header', async () => {
    const s = await startServer('alice:secret');
    try {
      const res = await request(s.url, basic('alice', 'secret'));
      expect(res.status).toBe(200);
    } finally {
      await s.close();
    }
  });

  it('returns 401 for a wrong password', async () => {
    const s = await startServer('alice:secret');
    try {
      const res = await request(s.url, basic('alice', 'wrong'));
      expect(res.status).toBe(401);
    } finally {
      await s.close();
    }
  });

  it('returns 401 for a wrong username', async () => {
    const s = await startServer('alice:secret');
    try {
      const res = await request(s.url, basic('bob', 'secret'));
      expect(res.status).toBe(401);
    } finally {
      await s.close();
    }
  });
});
