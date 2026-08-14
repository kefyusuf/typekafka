import { describe, expect, it, vi } from 'vitest';
import { withRetry } from '../src/retry.js';

describe('withRetry', () => {
  it('succeeds immediately when the task does not fail', async () => {
    const task = vi.fn().mockResolvedValue('ok');

    await expect(withRetry(task, { attempts: 3, baseDelayMs: 0 })).resolves.toBe('ok');
    expect(task).toHaveBeenCalledTimes(1);
  });

  it('retries until success', async () => {
    const task = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce('recovered');

    await expect(
      withRetry(task, { attempts: 3, baseDelayMs: 0, jitter: false }),
    ).resolves.toBe('recovered');
    expect(task).toHaveBeenCalledTimes(2);
  });

  it('exhausts attempts and re-throws the last error', async () => {
    const onAttempt = vi.fn();
    const task = vi.fn<() => Promise<string>>().mockRejectedValue(new Error('boom'));

    await expect(
      withRetry(task, { attempts: 3, baseDelayMs: 0, jitter: false }, onAttempt),
    ).rejects.toThrow('boom');

    expect(task).toHaveBeenCalledTimes(3);
    expect(onAttempt).toHaveBeenCalledTimes(2);
  });

  it('honors attempts of at least 1', async () => {
    const task = vi.fn<() => Promise<string>>().mockRejectedValue(new Error('boom'));

    await expect(
      withRetry(task, { attempts: 1, baseDelayMs: 0 }),
    ).rejects.toThrow('boom');
    expect(task).toHaveBeenCalledTimes(1);
  });
});
