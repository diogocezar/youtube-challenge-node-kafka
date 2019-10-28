import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'lead-group-server',
})

const topic = 'lead'
const consumer = kafka.consumer({ groupId: 'lead-group-server' })

const producer = kafka.producer();

async function run() {
  await consumer.connect()
  await consumer.subscribe({ topic })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
      const payload = JSON.parse(message.value);
      // setTimeout(() => {
      producer.send({
        topic: 'lead-response',
        messages: [
          { value: 'teste' }
        ]
      })
      // }, 3000);
    },
  })
}

run().catch(console.error)