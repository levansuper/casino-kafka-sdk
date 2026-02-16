import { KafkaClient, UserEvent, FinancialEvent } from '../src';

async function main() {
  const client = new KafkaClient({
    brokers: ['localhost:9092'],
    clientId: 'casino-example',
  });

  // --- Producer ---
  const producer = client.createProducer();
  await producer.connect();
  console.log('Producer connected');

  await producer.send(UserEvent.Login, {
    key: 'user-1',
    value: { userId: 'user-1', ip: '192.168.1.1' },
  });
  console.log('Login event sent');

  await producer.sendBatch(FinancialEvent.Transaction, [
    { key: 'user-2', value: { userId: 'user-2', amount: 50, currency: 'USD', transactionId: 'tx-001' } },
    { key: 'user-3', value: { userId: 'user-3', amount: 200, currency: 'USD', transactionId: 'tx-002' } },
  ]);
  console.log('Transaction batch sent');

  await producer.disconnect();

  // --- Consumer ---
  const consumer = client.createConsumer({ groupId: 'casino-example-group' });
  await consumer.connect();
  console.log('Consumer connected');

  await consumer.subscribe(
    FinancialEvent.Transaction,
    async (message) => {
      console.log(`Received [${message.key}]:`, message.value);
    },
    { fromBeginning: true },
  );

  // Let consumer run for 5 seconds then shut down
  setTimeout(async () => {
    await consumer.disconnect();
    console.log('Consumer disconnected');
    process.exit(0);
  }, 5000);
}

main().catch(console.error);
