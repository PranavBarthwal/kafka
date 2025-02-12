const { kafka } = require("./client");

async function init(){
    const admin = kafka.admin();
    console.log('Admin Connecting...');
    await admin.connect();
    console.log('Admin Connected!');

    console.log('Creating Topics...');
    await admin.createTopics({
        topics: [
            {topic: 'rider-updates', numPartitions: 2},
            {topic: 'driver-updates', numPartitions: 2},
        ]
    });
    console.log('Topics Created!');

    console.log('Disconnecting admin...');
    await admin.disconnect();
}

init()