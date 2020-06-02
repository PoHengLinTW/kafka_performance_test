
let kafka = require('kafka-node')
const config = require('./config')
const randstr = require('randomstring')
const performance = require('performance-measure')

let server_ip = '114.35.211.89'
let server_port = '9094'
var topic = 'promotion'
var part = 0
var attr = 0
var flag = false;

try {
    let Producer = kafka.HighLevelProducer
    let KeyedMessege = kafka.KeyedMessage
    let Client = kafka.KafkaClient
    let client = new Client({kafkaHost:config.ka_server, requestTimeout: 6000000})
    let options = [{

    }]
    let producer = new Producer(client)
    var keyMessage = new KeyedMessege('keyed', 'a keyed message')
    //const str_250k = randstr.generate(250*1024)
    const str_5m = randstr.generate(5*1024*1024)
    let payloads = [{
        topic: config.ka_topic,
        messages: [str_5m],//[str_250k],// keyMessage],
        partitions: 10
    }]

    //console.log('str_250k: ' + Object.keys(str_250k).length)
    console.log('str_5m: ' + Object.keys(str_5m).length)
    process.on('uncaughtException', function(err)  {
        console.error('uncaught error: ' + err)
        throw err
    })

    //client.refreshMetadata()
    let i=0;
    const m = new performance()
    producer.on('ready', async function () {
        console.log('ready')
        m.start('loop')
        for (i=0; i<config.testnum; i++) {
            producer.send(payloads, (err, data) => {
                console.log(err || data)
                if (err ) {
                    flag = true;
                }
                m.end('loop')
                console.log(m.print())
            })
            if (flag)
                break;
        }
    })
        
    producer.on('error', function (err) {
        console.error('Error: ' + err)
        throw err
    })
    
}
catch(e){
    console.log(e)
    process.exit()
}