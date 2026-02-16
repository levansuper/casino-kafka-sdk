import { describe, it, expect, vi, beforeEach } from 'vitest';
import { KafkaClient } from '../src/client';
import { SdkProducer } from '../src/producer';
import { SdkConsumer } from '../src/consumer';
import { FinancialEvent, ServerEvent } from '../src/types';

const mockAdmin = {
  connect: vi.fn(),
  disconnect: vi.fn(),
  createTopics: vi.fn(),
};

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
      admin() { return mockAdmin; }
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

  describe('ensureTopics', () => {
    beforeEach(() => {
      vi.clearAllMocks();
    });

    it('should return true when topics are created', async () => {
      mockAdmin.createTopics.mockResolvedValue(true);
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });

      const result = await client.ensureTopics([
        FinancialEvent.Transaction,
        FinancialEvent.Deposit,
      ]);

      expect(result).toBe(true);
      expect(mockAdmin.connect).toHaveBeenCalledOnce();
      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [
          { topic: FinancialEvent.Transaction, numPartitions: 1, replicationFactor: 1 },
          { topic: FinancialEvent.Deposit, numPartitions: 1, replicationFactor: 1 },
        ],
      });
      expect(mockAdmin.disconnect).toHaveBeenCalledOnce();
    });

    it('should return false when topics already exist', async () => {
      mockAdmin.createTopics.mockResolvedValue(false);
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });

      const result = await client.ensureTopics([FinancialEvent.Win]);

      expect(result).toBe(false);
    });

    it('should use custom numPartitions and replicationFactor', async () => {
      mockAdmin.createTopics.mockResolvedValue(true);
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });

      await client.ensureTopics(
        [ServerEvent.Crash],
        { numPartitions: 3, replicationFactor: 2 },
      );

      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [
          { topic: ServerEvent.Crash, numPartitions: 3, replicationFactor: 2 },
        ],
      });
    });

    it('should disconnect admin even if createTopics throws', async () => {
      mockAdmin.createTopics.mockRejectedValue(new Error('broker error'));
      const client = new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });

      await expect(client.ensureTopics([FinancialEvent.Loss])).rejects.toThrow('broker error');
      expect(mockAdmin.disconnect).toHaveBeenCalledOnce();
    });
  });
});
