import { timingSafeEqual } from 'node:crypto';
import type { RequestHandler } from 'express';

const REALM = 'typekafka';

interface Credentials {
  user: string;
  pass: string;
}

function splitCredentials(credentials: string): Credentials {
  const sep = credentials.indexOf(':');
  if (sep === -1) {
    return { user: credentials, pass: '' };
  }
  return { user: credentials.slice(0, sep), pass: credentials.slice(sep + 1) };
}

/**
 * Validates a raw `Authorization` header against `user:password` credentials
 * using constant-time comparison. Returns `true` only when both the username
 * and password match. Never logs the password.
 */
export function validateBasicCredentials(
  authHeader: string | undefined,
  credentials: string,
): boolean {
  if (!authHeader || !authHeader.startsWith('Basic ')) {
    return false;
  }

  const decoded = Buffer.from(authHeader.slice(6).trim(), 'base64');
  const sep = decoded.indexOf(0x3a); // ':'
  const user = sep === -1 ? decoded : decoded.subarray(0, sep);
  const pass = sep === -1 ? Buffer.alloc(0) : decoded.subarray(sep + 1);

  const expected = splitCredentials(credentials);
  const userBuf = Buffer.from(expected.user, 'utf8');
  const passBuf = Buffer.from(expected.pass, 'utf8');

  // Length checks happen before timingSafeEqual (which requires equal-length
  // buffers) so the comparison is constant-time for the compared portion and
  // short-circuits on length mismatch without leaking the password length beyond
  // whether it matched.
  const userOk = userBuf.length === user.length && timingSafeEqual(userBuf, user);
  const passOk = passBuf.length === pass.length && timingSafeEqual(passBuf, pass);

  return userOk && passOk;
}

/**
 * Express basic-auth middleware. When `credentials` is undefined or empty the
 * middleware is a no-op passthrough (demos run unchanged). Otherwise every
 * request must carry a valid `Authorization: Basic` header or it receives a
 * `401` with a `WWW-Authenticate` challenge. Health probes should be mounted
 * before this middleware is applied.
 */
export function createBasicAuthMiddleware(credentials?: string): RequestHandler {
  if (!credentials) {
    return (_req, _res, next) => next();
  }

  return (req, res, next) => {
    if (validateBasicCredentials(req.headers['authorization'], credentials)) {
      next();
      return;
    }
    res.setHeader('WWW-Authenticate', `Basic realm="${REALM}"`);
    res.statusCode = 401;
    res.end('Unauthorized');
  };
}
