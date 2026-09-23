import type { Response } from 'express';
import type { TelemetryEvent } from '@typekafka/domain';

export class SseHub {
  private clients = new Set<Response>();
  private heartbeat: NodeJS.Timeout | null = null;

  add(res: Response): void {
    this.clients.add(res);
    if (!this.heartbeat) {
      this.heartbeat = setInterval(() => {
        for (const client of this.clients) client.write(': ping\n\n');
      }, 15_000);
    }
  }

  remove(res: Response): void {
    this.clients.delete(res);
    if (this.clients.size === 0 && this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
  }

  broadcast(event: TelemetryEvent): void {
    const frame = `data: ${JSON.stringify(event)}\n\n`;
    for (const client of this.clients) client.write(frame);
  }

  /** Tear down: stop the heartbeat and end every open client stream. */
  close(): void {
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
    for (const client of this.clients) client.end();
    this.clients.clear();
  }
}
