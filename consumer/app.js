
'use strict';

import kafka from 'kafka-node';



const Consumer = kafka.Consumer;
const ConsumerGroup = kafka.ConsumerGroup;

var options = {
    kafkaHost: 'localhost:9092',
    groupId: 'testNew_group',
    fromOffset: "latest",
};
var consumerGroup = new ConsumerGroup(options, [
    'testNew'
]);

consumerGroup.on("message", function (message) {
        console.log(`message Poped ${JSON.stringify(message)}`);
});

consumerGroup.on('error', function (err) {
      console.log('error', err);
    });