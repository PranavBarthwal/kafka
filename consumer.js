const { kafka } = require('./client');
const group = process.argv[2];

async function init(){

    const consumer = kafka.consumer({groupId: 'group'});

    console.log('Connecting Consumer...');
    await consumer.connect();
    console.log('Consumer Connected!');

    await consumer.subscribe({ topic: 'rider-updates', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Rider Details: ${message.value.toString()} | Group: ${group} | Partition: ${partition} | Offset: ${message.offset} | Topic: ${topic}`);
        }
    })

}

init();