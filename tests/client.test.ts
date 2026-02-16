import { describe, it, expect, vi, beforeEach } from 'vitest';
import { KafkaClient } from '../src/client';
import { SdkProducer } from '../src/producer';
import { SdkConsumer } from '../src/consumer';
import { FinancialEvent, ServerEvent } from '../src/types';
import type { Logger } from '../src/logger';

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

  describe('logger', () => {
    let mockLogger: Logger;

    beforeEach(() => {
      vi.clearAllMocks();
      mockLogger = { error: vi.fn(), info: vi.fn(), debug: vi.fn() };
    });

    it('should log info on client creation with clientId and brokers', () => {
      new KafkaClient({
        brokers: ['localhost:9092'],
        clientId: 'test-app',
        logger: mockLogger,
      });

      expect(mockLogger.info).toHaveBeenCalledWith(
        'KafkaClient created',
        { clientId: 'test-app', brokers: ['localhost:9092'] },
      );
    });

    it('should log info on ensureTopics', async () => {
      mockAdmin.createTopics.mockResolvedValue(true);
      const client = new KafkaClient({
        brokers: ['localhost:9092'],
        clientId: 'test',
        logger: mockLogger,
      });

      await client.ensureTopics([FinancialEvent.Transaction, ServerEvent.Crash], {
        numPartitions: 3,
        replicationFactor: 2,
      });

      const infoCalls = (mockLogger.info as ReturnType<typeof vi.fn>).mock.calls;
      expect(infoCalls[1][0]).toBe('Ensuring topics exist');
      expect(infoCalls[1][1]).toEqual({
        topics: [FinancialEvent.Transaction, ServerEvent.Crash],
        numPartitions: 3,
        replicationFactor: 2,
      });
      expect(infoCalls[2][0]).toBe('ensureTopics completed');
      expect(infoCalls[2][1]).toEqual({ created: true });
    });

    it('should pass logger to created producer', () => {
      const client = new KafkaClient({
        brokers: ['localhost:9092'],
        clientId: 'test',
        logger: mockLogger,
      });

      const producer = client.createProducer();
      expect(producer).toBeInstanceOf(SdkProducer);
    });

    it('should pass logger to created consumer', () => {
      const client = new KafkaClient({
        brokers: ['localhost:9092'],
        clientId: 'test',
        logger: mockLogger,
      });

      const consumer = client.createConsumer({ groupId: 'test-group' });
      expect(consumer).toBeInstanceOf(SdkConsumer);
    });

    it('should not log when no logger is provided', () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

      new KafkaClient({ brokers: ['localhost:9092'], clientId: 'test' });

      expect(consoleSpy).not.toHaveBeenCalled();
      consoleSpy.mockRestore();
    });

    it('should allow per-producer logger override', () => {
      const clientLogger: Logger = { error: vi.fn(), info: vi.fn(), debug: vi.fn() };
      const producerLogger: Logger = { error: vi.fn(), info: vi.fn(), debug: vi.fn() };

      const client = new KafkaClient({
        brokers: ['localhost:9092'],
        clientId: 'test',
        logger: clientLogger,
      });

      const producer = client.createProducer({ logger: producerLogger });
      expect(producer).toBeInstanceOf(SdkProducer);
    });

    it('should allow per-consumer logger override', () => {
      const clientLogger: Logger = { error: vi.fn(), info: vi.fn(), debug: vi.fn() };
      const consumerLogger: Logger = { error: vi.fn(), info: vi.fn(), debug: vi.fn() };

      const client = new KafkaClient({
        brokers: ['localhost:9092'],
        clientId: 'test',
        logger: clientLogger,
      });

      const consumer = client.createConsumer({ groupId: 'test-group', logger: consumerLogger });
      expect(consumer).toBeInstanceOf(SdkConsumer);
    });
  });
});
