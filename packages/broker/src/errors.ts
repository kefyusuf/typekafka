export class BrokerError extends Error {
  constructor(
    message: string,
    override readonly cause?: unknown,
  ) {
    super(message, { cause });
    this.name = 'BrokerError';
  }
}

/** Thrown when the broker has already been connected / is not connected. */
export class BrokerStateError extends BrokerError {
  constructor(message: string) {
    super(message);
    this.name = 'BrokerStateError';
  }
}

/** Thrown when a message cannot be parsed / validated against its schema. */
export class MessageParseError extends BrokerError {
  constructor(
    message: string,
    readonly topic: string,
    readonly raw: unknown,
    cause?: unknown,
  ) {
    super(message, cause);
    this.name = 'MessageParseError';
  }
}

/** Thrown when a message handler fails in a retryable way. */
export class RetryableMessageError extends BrokerError {
  constructor(
    message: string,
    readonly topic: string,
    readonly retryCount: number,
    cause?: unknown,
  ) {
    super(message, cause);
    this.name = 'RetryableMessageError';
  }
}
