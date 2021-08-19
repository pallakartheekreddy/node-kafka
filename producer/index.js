import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const stream = Kafka.Producer.createWriteStream({
  'metadata.broker.list': 'localhost:9092'
}, {}, {
  topic: 'test'
});

stream.on('error', (err) => {
  console.error('Error in our kafka stream');
  console.error(err);
});

function queueRandomMessage(counter) {
  const category = getRandomAnimal();
  const noise = getRandomNoise(category, counter);
  const event = { category, noise };
  const success = stream.write(eventType.toBuffer(event));     
  if (success) {
    console.log(`message queued (${JSON.stringify(event)})`);
  } else {
    console.log('Too many messages in the queue already..');
  }
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
    return `${noises[Math.floor(Math.random() * noises.length)]}`;
  } else {
    return 'silence..';
  }
}
var counter = 0;
setInterval(() => {
  queueRandomMessage(counter++);
}, 3000);
