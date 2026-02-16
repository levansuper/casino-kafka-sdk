import { Producer } from 'kafkajs';
import { ProducerMessage, Serializer, Topic, TopicDataMap } from './types';
import { JsonSerializer } from './serializers';

export class SdkProducer {
  private producer: Producer;
  private serializer: Serializer;

  constructor(producer: Producer, serializer?: Serializer) {
    this.producer = producer;
    this.serializer = serializer ?? new JsonSerializer();
  }

  async connect(): Promise<void> {
    try {
      await this.producer.connect();
    } catch (error) {
      throw new Error(`Failed to connect producer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async disconnect(): Promise<void> {
    try {
      await this.producer.disconnect();
    } catch (error) {
      throw new Error(`Failed to disconnect producer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async send<T extends Topic>(topic: T, message: ProducerMessage<TopicDataMap[T]>): Promise<void> {
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
    } catch (error) {
      throw new Error(`Failed to send message to ${topic}: ${error instanceof Error ? error.message : error}`);
    }
  }

  async sendBatch<T extends Topic>(topic: T, messages: ProducerMessage<TopicDataMap[T]>[]): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: messages.map((msg) => ({
          key: msg.key ?? null,
          value: this.serializer.serialize(msg.value),
          headers: msg.headers,
        })),
      });
    } catch (error) {
      throw new Error(`Failed to send batch to ${topic}: ${error instanceof Error ? error.message : error}`);
    }
  }
}
