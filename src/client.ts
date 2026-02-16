import { Kafka } from 'kafkajs';
import { KafkaClientConfig, ProducerConfig, ConsumerConfig } from './types';
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
}
