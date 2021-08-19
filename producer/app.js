
'use strict';

import kafka from 'kafka-node';
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
// var Client = kafka.KafkaClient;
const client = new kafka.KafkaClient({
    kafkaHost: 'localhost:9092',
    maxAsyncRequests: 100
  })

// var client = new Client();
var topic = 'testNew';
var p = 0;
var a =  0;
var producer = new Producer(client, { requireAcks: 1 });

producer.on('ready', function () {
    console.log('Kafka Producer is connected and ready.')
});

producer.on('error', function (err) {
  console.log('error', err);
});

function queueRandomMessage(counter) {
    const category = getRandomAnimal();
    const noise = getRandomNoise(category, counter);
    const event = { category, noise };
    var keyedMessage = new KeyedMessage('keyed', 'a keyed message');

    producer.send([{ topic: topic, messages: [JSON.stringify(event)]}], 
    function (err,result) {
        console.log(`message queued (${JSON.stringify(event)}`);
    });
  }
  
  function getRandomAnimal() {
    const categories = ['CAT', 'DOG'];
    return categories[Math.floor(Math.random() * categories.length)];
  }
  
  function getRandomNoise(animal, counter) {
    if (animal === 'CAT') {
      const noises = ['meow', 'purr'];
      return `${noises[Math.floor(Math.random() * noises.length)]}_${counter}`;
    } else if (animal === 'DOG') {
      const noises = ['bark', 'woof'];
      return `${noises[Math.floor(Math.random() * noises.length)]}_${counter}`;
    } else {
      return 'silence..';
    }
  }
  var counter = 0;
  setInterval(() => {
    queueRandomMessage(counter++);
  }, 3000);

// const ConsumerGroup = kafka.ConsumerGroup;

// var options = {
//     kafkaHost: 'localhost:9092',
//     groupId: 'testNew_group',
//     fromOffset: "latest",
// };
// var consumerGroup = new ConsumerGroup(options, [
//     'testNew'
// ]);

// consumerGroup.on("message", function (message) {
//     console.log(`message Poped ${message}`);
// });

// consumerGroup.on('error', function (err) {
//     console.log('error', err);
// });