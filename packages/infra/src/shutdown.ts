import type { AppLogger } from './logger.js';

export interface Shutdownable {
  /** Name used in logs. */
  readonly name: string;
  shutdown(): Promise<void> | void;
}

interface ShutdownOptions {
  timeoutMs?: number;
  logger: AppLogger;
}

/**
 * Registers SIGINT/SIGTERM handlers that drain all registered resources in
 * reverse registration order, then exits. Used for graceful producer flush
 * and consumer offset commits before the process goes down.
 */
export function registerGracefulShutdown(
  resources: Shutdownable[],
  options: ShutdownOptions,
): void {
  const { timeoutMs = 10_000, logger } = options;
  let shuttingDown = false;

  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;

    logger.info({ signal }, 'graceful shutdown requested');

    const timer = setTimeout(() => {
      logger.error('graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, timeoutMs);
    timer.unref();

    try {
      for (const resource of [...resources].reverse()) {
        logger.info({ resource: resource.name }, 'shutting down resource');
        await resource.shutdown();
      }
      logger.info('graceful shutdown complete');
      process.exit(0);
    } catch (error) {
      logger.error({ err: error }, 'error during graceful shutdown');
      process.exit(1);
    } finally {
      clearTimeout(timer);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}
