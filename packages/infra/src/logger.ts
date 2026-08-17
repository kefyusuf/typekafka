import { pino, type Logger } from 'pino';
import { loadConfig } from './config.js';

export type AppLogger = Logger;

export function createLogger(level: string): AppLogger {
  const { serviceName } = loadConfig();
  return pino({
    level,
    base: {
      service: serviceName,
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
