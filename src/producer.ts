import { Producer } from 'kafkajs';
import { ProducerMessage, Serializer, Topic, TopicDataMap } from './types';
import { JsonSerializer } from './serializers';
import { Logger, noopLogger } from './logger';

export class SdkProducer {
  private producer: Producer;
  private serializer: Serializer;
  private logger: Logger;

  constructor(producer: Producer, serializer?: Serializer, logger?: Logger) {
    this.producer = producer;
    this.serializer = serializer ?? new JsonSerializer();
    this.logger = logger ?? noopLogger;
  }

  async connect(): Promise<void> {
    try {
      this.logger.info('Producer connecting');
      await this.producer.connect();
      this.logger.info('Producer connected');
    } catch (error) {
      this.logger.error('Failed to connect producer', {
        error: error instanceof Error ? error.message : String(error),
      });
      throw new Error(`Failed to connect producer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async disconnect(): Promise<void> {
    try {
      this.logger.info('Producer disconnecting');
      await this.producer.disconnect();
      this.logger.info('Producer disconnected');
    } catch (error) {
      this.logger.error('Failed to disconnect producer', {
        error: error instanceof Error ? error.message : String(error),
      });
      throw new Error(`Failed to disconnect producer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async send<T extends Topic>(topic: T, message: ProducerMessage<TopicDataMap[T]>): Promise<void> {
    this.logger.debug('Sending message', { topic, key: message.key ?? null });
    try {
      await this.producer.send({
        topic,
        messages: [
          {
            key: message.key ?? null,
            value: this.serializer.serialize(message.value),
            headers: message.headers,
          },
        ],
      });
      this.logger.debug('Message sent', { topic });
    } catch (error) {
      this.logger.error('Failed to send message', {
        topic,
        error: error instanceof Error ? error.message : String(error),
      });
      throw new Error(`Failed to send message to ${topic}: ${error instanceof Error ? error.message : error}`);
    }
  }

  async sendBatch<T extends Topic>(topic: T, messages: ProducerMessage<TopicDataMap[T]>[]): Promise<void> {
    this.logger.debug('Sending batch', { topic, messageCount: messages.length });
    try {
      await this.producer.send({
        topic,
        messages: messages.map((msg) => ({
          key: msg.key ?? null,
          value: this.serializer.serialize(msg.value),
          headers: msg.headers,
        })),
      });
      this.logger.debug('Batch sent', { topic, messageCount: messages.length });
    } catch (error) {
      this.logger.error('Failed to send batch', {
        topic,
        messageCount: messages.length,
        error: error instanceof Error ? error.message : String(error),
      });
      throw new Error(`Failed to send batch to ${topic}: ${error instanceof Error ? error.message : error}`);
    }
  }
}
