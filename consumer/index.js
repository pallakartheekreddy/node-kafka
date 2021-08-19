import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092'
}, {});

consumer.connect();
var counter = 0;
var numMessages = 5;
consumer.on('ready', () => {
  console.log('consumer ready..')
  consumer.subscribe(['testNew']);
  consumer.consume();
}).on('data', function(data) {
  consumer.commit(data);
  // counter++;

  // //committing offsets every numMessages
  // if (counter % numMessages === 0) {
  //   console.log(`calling commit:  ${eventType.fromBuffer(data.value)}`);
  //   consumer.commit(data);
  // }
  // Output the actual message contents
  console.log(`received message: ${eventType.fromBuffer(data.value)}`);
});
