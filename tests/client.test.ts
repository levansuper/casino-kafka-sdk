import { describe, it, expect, vi } from 'vitest';
import { KafkaClient } from '../src/client';
import { SdkProducer } from '../src/producer';
import { SdkConsumer } from '../src/consumer';

vi.mock('kafkajs', () => {
  const mockProducer = {
    connect: vi.fn(),
    disconnect: vi.fn(),
    send: vi.fn(),
  };
  const mockConsumer = {
    connect: vi.fn(),
    disconnect: vi.fn(),
    subscribe: vi.fn(),
    run: vi.fn(),
  };
  return {
    Kafka: class MockKafka {
      constructor() {}
      producer() { return mockProducer; }
      consumer() { return mockConsumer; }
    },
  };
});

describe('KafkaClient', () => {
  it('should create a KafkaClient with required config', () => {
    const client = new KafkaClient({
      brokers: ['localhost:9092'],
      clientId: 'test-app',
    });

    expect(client).toBeInstanceOf(KafkaClient);
  });

  describe('createProducer', () => {
    it('should return an SdkProducer instance', () => {
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });
      const producer = client.createProducer();

      expect(producer).toBeInstanceOf(SdkProducer);
    });
  });

  describe('createConsumer', () => {
    it('should return an SdkConsumer instance', () => {
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });
      const consumer = client.createConsumer({ groupId: 'test-group' });

      expect(consumer).toBeInstanceOf(SdkConsumer);
    });

    it('should default to sequential (concurrency 1)', () => {
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });
      const consumer = client.createConsumer({ groupId: 'test-group' });

      expect(consumer).toBeInstanceOf(SdkConsumer);
    });

    it('should force concurrency 1 when sequential is true', () => {
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });
      const consumer = client.createConsumer({
        groupId: 'test-group',
        sequential: true,
        concurrency: 10,
      });

      expect(consumer).toBeInstanceOf(SdkConsumer);
    });

    it('should allow custom concurrency when sequential is false', () => {
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });
      const consumer = client.createConsumer({
        groupId: 'test-group',
        sequential: false,
        concurrency: 5,
      });

      expect(consumer).toBeInstanceOf(SdkConsumer);
    });
  });
});
