import { Consumer } from 'kafkajs';
import { MessageHandler, Serializer, SubscribeOptions, Topic, TopicDataMap } from './types';
import { JsonSerializer } from './serializers';

export class SdkConsumer {
  private consumer: Consumer;
  private serializer: Serializer;
  private concurrency: number;
  private propagateErrors: boolean;

  constructor(consumer: Consumer, serializer?: Serializer, concurrency?: number, propagateErrors?: boolean) {
    this.consumer = consumer;
    this.serializer = serializer ?? new JsonSerializer();
    this.concurrency = concurrency ?? 1;
    this.propagateErrors = propagateErrors ?? true;
  }

  async connect(): Promise<void> {
    try {
      await this.consumer.connect();
    } catch (error) {
      throw new Error(`Failed to connect consumer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async disconnect(): Promise<void> {
    try {
      await this.consumer.disconnect();
    } catch (error) {
      throw new Error(`Failed to disconnect consumer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async subscribe<T extends Topic>(
    topic: T,
    handler: MessageHandler<T>,
    options?: SubscribeOptions,
  ): Promise<void> {
    await this.consumer.subscribe({
      topic,
      fromBeginning: options?.fromBeginning ?? false,
    });

    await this.consumer.run({
      partitionsConsumedConcurrently: this.concurrency,
      eachMessage: async ({ topic: msgTopic, partition, message }) => {
        try {
          const value = message.value
            ? this.serializer.deserialize(message.value as Buffer) as TopicDataMap[T]
            : (null as unknown as TopicDataMap[T]);

          const headers: Record<string, string | undefined> = {};
          if (message.headers) {
            for (const [key, val] of Object.entries(message.headers)) {
              headers[key] = val?.toString();
            }
          }

          await handler({
            topic: msgTopic as T,
            partition,
            offset: message.offset,
            key: message.key?.toString() ?? null,
            value,
            headers,
            timestamp: message.timestamp ?? '',
          });
        } catch (error) {
          if (this.propagateErrors) {
            throw error;
          }
          console.error(
            `Error processing message from ${msgTopic}[${partition}] at offset ${message.offset}:`,
            error,
          );
        }
      },
    });
  }
}
