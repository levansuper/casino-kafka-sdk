import { describe, it, expect, vi, beforeEach } from 'vitest';
import { SdkProducer } from '../src/producer';
import { FinancialEvent, UserEvent } from '../src/types';

function createMockProducer() {
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    send: vi.fn().mockResolvedValue(undefined),
  };
}

describe('SdkProducer', () => {
  let mockProducer: ReturnType<typeof createMockProducer>;
  let producer: SdkProducer;

  beforeEach(() => {
    mockProducer = createMockProducer();
    producer = new SdkProducer(mockProducer as any);
  });

  describe('connect', () => {
    it('should call producer.connect', async () => {
      await producer.connect();
      expect(mockProducer.connect).toHaveBeenCalledOnce();
    });

    it('should throw a descriptive error on failure', async () => {
      mockProducer.connect.mockRejectedValueOnce(new Error('broker down'));
      await expect(producer.connect()).rejects.toThrow('Failed to connect producer: broker down');
    });
  });

  describe('disconnect', () => {
    it('should call producer.disconnect', async () => {
      await producer.disconnect();
      expect(mockProducer.disconnect).toHaveBeenCalledOnce();
    });

    it('should throw a descriptive error on failure', async () => {
      mockProducer.disconnect.mockRejectedValueOnce(new Error('timeout'));
      await expect(producer.disconnect()).rejects.toThrow('Failed to disconnect producer: timeout');
    });
  });

  describe('send', () => {
    it('should send a single message with correct topic and serialized value', async () => {
      await producer.send(UserEvent.Login, {
        key: 'user-1',
        value: { userId: 'user-1', ip: '10.0.0.1' },
      });

      expect(mockProducer.send).toHaveBeenCalledOnce();
      const call = mockProducer.send.mock.calls[0][0];
      expect(call.topic).toBe('user-event.login');
      expect(call.messages).toHaveLength(1);
      expect(call.messages[0].key).toBe('user-1');

      const value = JSON.parse(call.messages[0].value.toString());
      expect(value).toEqual({ userId: 'user-1', ip: '10.0.0.1' });
    });

    it('should default key to null when not provided', async () => {
      await producer.send(UserEvent.Logout, {
        value: { userId: 'user-1' },
      });

      const call = mockProducer.send.mock.calls[0][0];
      expect(call.messages[0].key).toBeNull();
    });

    it('should pass headers through', async () => {
      await producer.send(UserEvent.Login, {
        key: 'u1',
        value: { userId: 'u1', ip: '1.2.3.4' },
        headers: { 'x-trace-id': 'abc-123' },
      });

      const call = mockProducer.send.mock.calls[0][0];
      expect(call.messages[0].headers).toEqual({ 'x-trace-id': 'abc-123' });
    });

    it('should throw a descriptive error on failure', async () => {
      mockProducer.send.mockRejectedValueOnce(new Error('network error'));
      await expect(
        producer.send(FinancialEvent.Win, { value: { userId: 'u1', amount: 100, gameId: 'g1' } }),
      ).rejects.toThrow('Failed to send message to financial-event.win: network error');
    });
  });

  describe('sendBatch', () => {
    it('should send multiple messages in a single call', async () => {
      await producer.sendBatch(FinancialEvent.Transaction, [
        { key: 'u1', value: { userId: 'u1', amount: 50, currency: 'USD', transactionId: 'tx-1' } },
        { key: 'u2', value: { userId: 'u2', amount: 100, currency: 'EUR', transactionId: 'tx-2' } },
      ]);

      expect(mockProducer.send).toHaveBeenCalledOnce();
      const call = mockProducer.send.mock.calls[0][0];
      expect(call.topic).toBe('financial-event.transaction');
      expect(call.messages).toHaveLength(2);

      const v0 = JSON.parse(call.messages[0].value.toString());
      const v1 = JSON.parse(call.messages[1].value.toString());
      expect(v0.transactionId).toBe('tx-1');
      expect(v1.transactionId).toBe('tx-2');
    });

    it('should throw a descriptive error on failure', async () => {
      mockProducer.send.mockRejectedValueOnce(new Error('quota exceeded'));
      await expect(
        producer.sendBatch(FinancialEvent.Transaction, [
          { value: { userId: 'u1', amount: 1, currency: 'USD', transactionId: 'tx-1' } },
        ]),
      ).rejects.toThrow('Failed to send batch to financial-event.transaction: quota exceeded');
    });
  });
});
