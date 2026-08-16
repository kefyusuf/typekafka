import type { KafkaMessage } from '@nodejs-kafka/broker';
import type { CustomerUpdated } from '@nodejs-kafka/domain';

export class CustomerStore {
  private readonly customers = new Map<string, CustomerUpdated>();

  apply(message: KafkaMessage<CustomerUpdated | null>): void {
    const key = message.key;
    if (key === null) {
      throw new Error('customer changelog records must be keyed by customerId');
    }
    if (message.value === null) {
      this.customers.delete(key);
      return;
    }
    this.customers.set(key, message.value);
  }

  get(customerId: string): CustomerUpdated | undefined {
    return this.customers.get(customerId);
  }

  list(): CustomerUpdated[] {
    return Array.from(this.customers.values());
  }

  delete(customerId: string): boolean {
    return this.customers.delete(customerId);
  }

  get size(): number {
    return this.customers.size;
  }
}
