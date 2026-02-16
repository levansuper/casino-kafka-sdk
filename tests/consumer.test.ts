import { describe, it, expect, vi, beforeEach } from 'vitest';
import { SdkConsumer } from '../src/consumer';
import { FinancialEvent, UserEvent, type ConsumedMessage } from '../src/types';

function createMockConsumer() {
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn().mockResolvedValue(undefined),
  };
}

describe('SdkConsumer', () => {
  let mockConsumer: ReturnType<typeof createMockConsumer>;
  let consumer: SdkConsumer;

  beforeEach(() => {
    mockConsumer = createMockConsumer();
    consumer = new SdkConsumer(mockConsumer as any);
  });

  describe('connect', () => {
    it('should call consumer.connect', async () => {
      await consumer.connect();
      expect(mockConsumer.connect).toHaveBeenCalledOnce();
    });

    it('should throw a descriptive error on failure', async () => {
      mockConsumer.connect.mockRejectedValueOnce(new Error('broker unreachable'));
      await expect(consumer.connect()).rejects.toThrow('Failed to connect consumer: broker unreachable');
    });
  });

  describe('disconnect', () => {
    it('should call consumer.disconnect', async () => {
      await consumer.disconnect();
      expect(mockConsumer.disconnect).toHaveBeenCalledOnce();
    });

    it('should throw a descriptive error on failure', async () => {
      mockConsumer.disconnect.mockRejectedValueOnce(new Error('timeout'));
      await expect(consumer.disconnect()).rejects.toThrow('Failed to disconnect consumer: timeout');
    });
  });

  describe('subscribe', () => {
    it('should subscribe to the correct topic', async () => {
      await consumer.subscribe(FinancialEvent.Win, vi.fn());

      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'financial-event.win',
        fromBeginning: false,
      });
    });

    it('should pass fromBeginning option', async () => {
      await consumer.subscribe(FinancialEvent.Win, vi.fn(), { fromBeginning: true });

      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'financial-event.win',
        fromBeginning: true,
      });
    });

    it('should call consumer.run with eachMessage handler', async () => {
      await consumer.subscribe(FinancialEvent.Win, vi.fn());

      expect(mockConsumer.run).toHaveBeenCalledOnce();
      const runConfig = mockConsumer.run.mock.calls[0][0];
      expect(runConfig.eachMessage).toBeTypeOf('function');
      expect(runConfig.partitionsConsumedConcurrently).toBe(1);
    });

    it('should deserialize and pass message to handler', async () => {
      const handler = vi.fn();
      await consumer.subscribe(UserEvent.Login, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await eachMessage({
        topic: 'user-event.login',
        partition: 0,
        message: {
          offset: '42',
          key: Buffer.from('user-1'),
          value: Buffer.from(JSON.stringify({ userId: 'user-1', ip: '10.0.0.1' })),
          headers: {},
          timestamp: '1700000000000',
        },
      });

      expect(handler).toHaveBeenCalledOnce();
      const msg: ConsumedMessage<typeof UserEvent.Login> = handler.mock.calls[0][0];
      expect(msg.topic).toBe('user-event.login');
      expect(msg.partition).toBe(0);
      expect(msg.offset).toBe('42');
      expect(msg.key).toBe('user-1');
      expect(msg.value).toEqual({ userId: 'user-1', ip: '10.0.0.1' });
      expect(msg.timestamp).toBe('1700000000000');
    });

    it('should handle null message key', async () => {
      const handler = vi.fn();
      await consumer.subscribe(UserEvent.Logout, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await eachMessage({
        topic: 'user-event.logout',
        partition: 0,
        message: {
          offset: '0',
          key: null,
          value: Buffer.from(JSON.stringify({ userId: 'u1' })),
          headers: {},
          timestamp: '1700000000000',
        },
      });

      expect(handler.mock.calls[0][0].key).toBeNull();
    });

    it('should handle null message value', async () => {
      const handler = vi.fn();
      await consumer.subscribe(UserEvent.Logout, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await eachMessage({
        topic: 'user-event.logout',
        partition: 0,
        message: {
          offset: '0',
          key: null,
          value: null,
          headers: {},
          timestamp: '1700000000000',
        },
      });

      expect(handler.mock.calls[0][0].value).toBeNull();
    });

    it('should convert message headers to strings', async () => {
      const handler = vi.fn();
      await consumer.subscribe(UserEvent.Login, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await eachMessage({
        topic: 'user-event.login',
        partition: 0,
        message: {
          offset: '0',
          key: null,
          value: Buffer.from(JSON.stringify({ userId: 'u1', ip: '1.1.1.1' })),
          headers: { 'x-trace-id': Buffer.from('trace-abc') },
          timestamp: '1700000000000',
        },
      });

      expect(handler.mock.calls[0][0].headers).toEqual({ 'x-trace-id': 'trace-abc' });
    });

    it('should default timestamp to empty string when undefined', async () => {
      const handler = vi.fn();
      await consumer.subscribe(UserEvent.Login, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await eachMessage({
        topic: 'user-event.login',
        partition: 0,
        message: {
          offset: '0',
          key: null,
          value: Buffer.from(JSON.stringify({ userId: 'u1', ip: '1.1.1.1' })),
          headers: {},
          timestamp: undefined,
        },
      });

      expect(handler.mock.calls[0][0].timestamp).toBe('');
    });

    it('should propagate handler errors by default so KafkaJS does not commit the offset', async () => {
      const handler = vi.fn().mockRejectedValueOnce(new Error('handler boom'));

      await consumer.subscribe(UserEvent.Login, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await expect(
        eachMessage({
          topic: 'user-event.login',
          partition: 0,
          message: {
            offset: '5',
            key: null,
            value: Buffer.from(JSON.stringify({ userId: 'u1', ip: '1.1.1.1' })),
            headers: {},
            timestamp: '1700000000000',
          },
        }),
      ).rejects.toThrow('handler boom');
    });

    it('should swallow handler errors when propagateErrors is false', async () => {
      const swallowConsumer = new SdkConsumer(mockConsumer as any, undefined, undefined, false);
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      const handler = vi.fn().mockRejectedValueOnce(new Error('handler boom'));

      await swallowConsumer.subscribe(UserEvent.Login, handler);

      const eachMessage = mockConsumer.run.mock.calls[0][0].eachMessage;
      await eachMessage({
        topic: 'user-event.login',
        partition: 0,
        message: {
          offset: '5',
          key: null,
          value: Buffer.from(JSON.stringify({ userId: 'u1', ip: '1.1.1.1' })),
          headers: {},
          timestamp: '1700000000000',
        },
      });

      expect(consoleSpy).toHaveBeenCalledOnce();
      expect(consoleSpy.mock.calls[0][0]).toContain('user-event.login');
      expect(consoleSpy.mock.calls[0][0]).toContain('offset 5');
      consoleSpy.mockRestore();
    });
  });

  describe('concurrency', () => {
    it('should use default concurrency of 1', async () => {
      await consumer.subscribe(FinancialEvent.Win, vi.fn());

      const runConfig = mockConsumer.run.mock.calls[0][0];
      expect(runConfig.partitionsConsumedConcurrently).toBe(1);
    });

    it('should use custom concurrency when provided', async () => {
      const concurrentConsumer = new SdkConsumer(mockConsumer as any, undefined, 5);
      await concurrentConsumer.subscribe(FinancialEvent.Win, vi.fn());

      const runConfig = mockConsumer.run.mock.calls[0][0];
      expect(runConfig.partitionsConsumedConcurrently).toBe(5);
    });
  });
});
