import { Kafka } from 'kafkajs';
import { KafkaClientConfig, ProducerConfig, ConsumerConfig, Topic } from './types';
import { SdkProducer } from './producer';
import { SdkConsumer } from './consumer';
import { Logger, noopLogger } from './logger';

export class KafkaClient {
  private kafka: Kafka;
  private logger: Logger;

  constructor(config: KafkaClientConfig) {
    this.logger = config.logger ?? noopLogger;
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
      logLevel: config.logLevel,
      ...config.kafkaOptions,
    });
    this.logger.info('KafkaClient created', {
      clientId: config.clientId,
      brokers: config.brokers,
    });
  }

  createProducer(config?: ProducerConfig): SdkProducer {
    return new SdkProducer(
      this.kafka.producer(),
      config?.serializer,
      config?.logger ?? this.logger,
    );
  }

  createConsumer(config: ConsumerConfig): SdkConsumer {
    const concurrency = config.sequential !== false ? 1 : (config.concurrency ?? 1);
    return new SdkConsumer(
      this.kafka.consumer({ groupId: config.groupId }),
      config.serializer,
      concurrency,
      config.propagateErrors,
      config?.logger ?? this.logger,
    );
  }

  async ensureTopics(
    topics: Topic[],
    options?: { numPartitions?: number; replicationFactor?: number },
  ): Promise<boolean> {
    this.logger.info('Ensuring topics exist', {
      topics,
      numPartitions: options?.numPartitions ?? 1,
      replicationFactor: options?.replicationFactor ?? 1,
    });
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const result = await admin.createTopics({
        topics: topics.map((topic) => ({
          topic,
          numPartitions: options?.numPartitions ?? 1,
          replicationFactor: options?.replicationFactor ?? 1,
        })),
      });
      this.logger.info('ensureTopics completed', { created: result });
      return result;
    } finally {
      await admin.disconnect();
    }
  }
}
