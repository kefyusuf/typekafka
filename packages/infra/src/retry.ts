import { setTimeout as sleep } from 'node:timers/promises';

export interface RetryOptions {
  attempts: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
  jitter?: boolean;
}

export interface RetryState {
  attempt: number;
  delayMs: number;
}

export const DEFAULT_RETRY: Required<Omit<RetryOptions, 'attempts'>> = {
  baseDelayMs: 100,
  maxDelayMs: 10_000,
  factor: 2,
  jitter: true,
};

/**
 * Run `task` with exponential backoff + full jitter.
 * Re-throws the last error when all attempts are exhausted.
 */
export async function withRetry<T>(
  task: () => Promise<T>,
  options: RetryOptions,
  onAttempt?: (state: RetryState, error: unknown) => void,
): Promise<T> {
  const opts = { ...DEFAULT_RETRY, ...options };
  const attempts = Math.max(1, opts.attempts);

  let lastError: unknown;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await task();
    } catch (error) {
      lastError = error;
      if (attempt === attempts) break;

      const base = opts.baseDelayMs * Math.pow(opts.factor, attempt - 1);
      const capped = Math.min(base, opts.maxDelayMs);
      const delay = opts.jitter ? capped * Math.random() : capped;

      onAttempt?.({ attempt, delayMs: Math.round(delay) }, error);
      await sleep(delay);
    }
  }

  throw lastError;
}
