
let kafka = require('kafka-node')
let HLconsumer = kafka.Consumer

const config = require('./config')

let options = {
    groupId: 'node1',
    fromOffset: 'false',
    sessionTimeout: 6000,
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024 * 50 // 5Mb as a unit
    //partitions: 10
}

let topics = [{
    topic: config.ka_topic
}]
var cnt = 0
let client = new kafka.KafkaClient({kafkaHost: config.ka_server})//kafkaHost: server_ip + ':' + server_port})
let consumer = new kafka.Consumer(client, topics, options)
let offset = new kafka.Offset(client)

consumer.on('message', function (message) {
    console.log(cnt++)
    /*console.log('topic:             ' + message['topic'])
    console.log('offset:            ' + message['offset'])
    console.log('partition:         ' + message['partition'])
    console.log('highWaterOffset:   ' + message['highWaterOffset'])
    console.log('====================================================')*/
})

consumer.on('error', function (err) {
    console.error("error: " + err)
})

consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offset) {
        if (err)
            return console.error(err);
        var min = Math.min.apply(null, offset[topic.topic][topic.partition])
        consumer.setOffset(topic.topic, topic.partition, min)
    })
})