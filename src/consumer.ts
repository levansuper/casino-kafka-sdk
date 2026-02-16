import { Consumer } from 'kafkajs';
import { MessageHandler, Serializer, SubscribeOptions, Topic } from './types';
import { JsonSerializer } from './serializers';

export class SdkConsumer {
  private consumer: Consumer;
  private serializer: Serializer;
  private concurrency: number;
  private propagateErrors: boolean;
  private handlers: Map<string, MessageHandler<any>> = new Map();
  private running = false;

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
      this.running = false;
      this.handlers.clear();
    } catch (error) {
      throw new Error(`Failed to disconnect consumer: ${error instanceof Error ? error.message : error}`);
    }
  }

  async subscribe<T extends Topic>(
    topic: T,
    handler: MessageHandler<T>,
    options?: SubscribeOptions,
  ): Promise<void> {
    if (this.running) {
      throw new Error(
        `Cannot subscribe to topic "${topic}" while consumer is already running. Call subscribe() before run().`,
      );
    }

    await this.consumer.subscribe({
      topic,
      fromBeginning: options?.fromBeginning ?? false,
    });

    this.handlers.set(topic, handler);
  }

  async run(): Promise<void> {
    if (this.running) {
      throw new Error('Consumer is already running.');
    }
    if (this.handlers.size === 0) {
      throw new Error('No topics subscribed. Call subscribe() before run().');
    }

    this.running = true;

    await this.consumer.run({
      partitionsConsumedConcurrently: this.concurrency,
      eachMessage: async ({ topic: msgTopic, partition, message }) => {
        const handler = this.handlers.get(msgTopic);
        if (!handler) {
          return;
        }

        try {
          const value = message.value
            ? this.serializer.deserialize(message.value as Buffer)
            : null;

          const headers: Record<string, string | undefined> = {};
          if (message.headers) {
            for (const [key, val] of Object.entries(message.headers)) {
              headers[key] = val?.toString();
            }
          }

          await handler({
            topic: msgTopic as Topic,
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
