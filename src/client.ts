import { Kafka } from 'kafkajs';
import { KafkaClientConfig, ProducerConfig, ConsumerConfig, Topic } from './types';
import { SdkProducer } from './producer';
import { SdkConsumer } from './consumer';

export class KafkaClient {
  private kafka: Kafka;

  constructor(config: KafkaClientConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
      logLevel: config.logLevel,
      ...config.kafkaOptions,
    });
  }

  createProducer(config?: ProducerConfig): SdkProducer {
    return new SdkProducer(this.kafka.producer(), config?.serializer);
  }

  createConsumer(config: ConsumerConfig): SdkConsumer {
    const concurrency = config.sequential !== false ? 1 : (config.concurrency ?? 1);
    return new SdkConsumer(
      this.kafka.consumer({ groupId: config.groupId }),
      config.serializer,
      concurrency,
      config.propagateErrors,
    );
  }

  async ensureTopics(
    topics: Topic[],
    options?: { numPartitions?: number; replicationFactor?: number },
  ): Promise<boolean> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      return await admin.createTopics({
        topics: topics.map((topic) => ({
          topic,
          numPartitions: options?.numPartitions ?? 1,
          replicationFactor: options?.replicationFactor ?? 1,
        })),
      });
    } finally {
      await admin.disconnect();
    }
  }
}
