const logger = new (require('node-red-contrib-logger'))('Kafka Producer');
logger.sendInfo('Copyright 2022 Jaroslav Peter Prib');
const setupHttpAdmin = require('./setupHttpAdmin.js');
const evalInFunction = require('./evalInFunction.js');
const getDataType = require('./getDataType.js');
const zlib = require('node:zlib');
const compressionTool = require('compressiontool');
let kafka;

function messageAttributes(msg){
  return {
    messageDataType: getDataType(msg.messages[0]),
    topic: msg.topic,
    key: msg.key,
    partition: msg.partition,
    attributes: msg.attributes,
  }
}

function producerSend(node, msgIn, retry=0) {
  if (logger.active) logger.send({ label: 'producerSend', node: node.id, retry: retry, msg:sendMsgToString })
  /*
  //				node.KeyedMessage = kafka.KeyedMessage
  //	km = new KeyedMessage('key', 'message'),
  //	payloads = [
  //			{ topic: 'topic1', messages: 'hi', partition: 0 },
  //			{ topic: 'topic2', messages: ['hello', 'world', km] }
  //	];

{
topic: 'topicName',
messages: ['message body'], // multi messages should be a array, single message can be just a string or a KeyedMessage instance
key: 'theKey', // string or buffer, only needed when using keyed partitioner
partition: 0, // default 0
attributes: 2, // default: 0
timestamp: Date.now() // <-- defaults to Date.now() (only available with kafka v0.10+)
}

attributes:
0: No compression
1: Compress using GZip
2: Compress using snappy
*/
  try {
    let msg
    if (retry && msgIn) {
      msg = msgIn;
    } else {
      if (node.waiting.length) {
        if (msgIn) node.waiting.push(msgIn) //get oldest message
        msg = node.waiting.shift()
      } else {
        if (!msgIn) return
        msg = msgIn
      }
    }
    node.producer.send([msg],
      function (err, data) {
        if (err) {
          let errmsg = (typeof err === 'string' ? err : err.message)
          if (logger.active) logger.error({label: 'producerSend',
            error: errmsg, node: node.id, retry: retry,messageAttributes:messageAttributes(msg),
            stack: (typeof err === 'string' ? undefined : err.stack)
          });
          if (errmsg.startsWith('Could not find the leader')
            || errmsg.startsWith('Broker not available')) {
            node.log(errmsg)
            node.inError = true
            if (retry>0) {
              node.retryCount++
              setInError(node, 'retry ' + node.retryCount + ' failed, waiting: ' + node.waiting.length)
              node.sendDeadletter(msg);
              return;
            }
            if (node.client.refreshMetadata) {
              node.log('issuing refreshMetadata as may be issue with caching')
              node.client.refreshMetadata([node.topic], (err) => {
                if (err) {
                  setInError(node, errmsg)
                  msg.error=err;
                  node.sendDeadletter(msg);
                  return
                }
                if (node.waiting.length) setStatus(node, 'trying sending ' + node.waiting.length + ' queued messages', 'yellow')
                producerSend(node, msg,++retry)
                if (node.waiting.length) {
                  setStatus(node, 'retry send to Kafka failed, ' + node.waiting.length + ' queued messages', 'red');
                } else {
                  setStatus(node, 'Connected to ' + node.brokerNode.name)
                }
              })
              return
            } else if (errmsg.startsWith('Could not find the leader')) {
              msg.error=err;
              node.sendDeadletter(msg);
              return;
            } else {
              node.connected = false
              node.queueMsg(msg)
            }
          } else if (errmsg.startsWith('InvalidTopic')) {
            errmsg = "Invalid topic " + msg.topic;
          }
          setInError(node, errmsg);
          msg.error=err;
          node.sendDeadletter(msg);
        } else {
          node.successes++
          if (node.inError) {
            node.inError = false
            setStatus(node, 'Connected to ' + node.brokerNode.name)
          }
          if (node.waiting) producerSend(node);
        }
      })
  } catch (ex) {
    node.inError = true;
    if (logger.active) logger.error({label: 'producerSend',
      error: ex.message, node: node.id, retry: retry,messageAttributes:messageAttributes(msg),
      stack: ex.stack
    });
    setStatus(node, 'send error ' + ex.toString(), 'yellow')
  }
}

function setStatus(node, msg, color = "green") {
  node.statusText = msg;
  node.statusfill = color
  if (color == "red") node.error(msg)
  else if (color == "yellow") node.warn(msg);
  showStatus(node);
}
function showStatus(node) {
  node.status({ fill: node.statusfill, shape: 'ring', text: node.statusText })
}
function setInError(node, errmsg) {
  setStatus(node, 'send error ' + errmsg, "red")
  node.inError = true
}
function connect(node) {
  if (logger.active) logger.send({ label: 'connect', node: node.id });
  if (node.connected) throw Error("Already connected");
  if (!node.client) node.client = node.brokerNode.getKafkaClient();
  if (node.connecting == true) {
    if (logger.active) logger.send({ label: 'connect', node: node.id, warning: "already connecting" });
    throw Error("Already connecting")
  }
  node.connecting = true;
  node.producer = new kafka[(node.connectionType || 'Producer')](node.client, {
    // Configuration for when to consider a message as acknowledged, default 1
    requireAcks: node.partitionerType || 1,
    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
    ackTimeoutMs: node.partitionerType || 100,
    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
    partitionerType: node.partitionerType || 0
  })
  node.statusText = 'Waiting on ' + node.brokerNode.name;
  node.statusfill = "yellow";
  node.status({ fill: node.statusfill, shape: 'ring', text: node.statusText })
  node.producer.on('error', function (ex) {
    const errMsg = "connect error " + node.brokerNode.getRevisedMessage(ex.message)
    logger.error({ label: 'connect error', node: node.id, error: ex.message, errorEnhanced: errMsg });
    node.connecting = false;
    setStatus(node, errMsg, "red")
  })
  node.producer.on('close', function () {
    logger.info({ label: 'connect close', node: node.id, name: node.name });
    node.connecting = false;
    node.connected = false;
  })
  node.producer.on('ready', function () {
    node.connected = true
    node.connecting = false;
    setStatus(node, 'sending to ' + node.brokerNode.name);
    node.log('connected and processing ' + node.waiting.length + ' messages')
    producerSend(node, undefined);
  });
}
function sendDeadletter(msg) {
  const node = this;
  const message = JSON.stringify(msg);
  try {
    zlib.gzip(message, (err, buffer) => {
      if (err) {
        logger.error({ label: "deadletter", error: err, deadletterTopic: node.deadletterTopic })
        node.error("dead letter failed turned off");
        node.sendDeadletter = () => node.errorDiscarded++;
        return;
      }
      const deadletterMsg = {
        topic: node.deadletterTopic || "deadletter",
        messages: [buffer],
        attributes: 0
      }
      node.producer.send([deadletterMsg],
        function (err, data) {
          if (err) {
            logger.error({ label: "deadletter", error: err, deadletterTopic: node.deadletterTopic })
            node.error("dead letter failed turned off");
            node.sendDeadletter = () => node.errorDiscarded++;
          } else
            node.deadletters++;
          if (node.waiting) producerSend(node, undefined);
        });

    });
  } catch (ex) {
    logger.error({ label: "deadletter", error: ex.message, deadletterTopic: node.deadletterTopic })
    node.error("dead letter failed turned off");
    node.sendDeadletter = () => node.errorDiscarded++;
  }
}
function sendMsgToString(msg){
  return {topic:msg.topic, key:msg.key,partition:msg.partition,attributes: msg.attributes}.toString()
}
function processMessageNoCompression(node, msg, msgData) {
  if(logger.active) logger.send({label:"processMessageNoCompression",msg:msg})
  try {
    const msgTopic = node.topicSlash2dot && msg.topic ? msg.topic.replace('/', '.') : msg.topic
    const sendMsg = {
      topic: msgTopic || node.topic || '',
      messages: [msgData],
      key: msg.key || node.getKey(msg),
      partition: msg.partition || node.partition || 0,
      attributes: msg.attributes || node.attributes || 0
    };
    if (node.connected) {
      producerSend(node, sendMsg)
      return
    }
    node.queueMsg(msg)
  } catch (ex) {
    if (logger.active) logger.sendErrorAndStackDump(ex.message, ex)
    node.error('input error:' + ex.message)
  }
}
function processMessageCompression(node, msg, msgData) {
  node.compressor.compress(msgData,
    (compressed) => processMessageNoCompression(node, msg, compressed),
    (err) => {
      if((node.compressionError++)==1){
        node.warn("compression failure(s)")
      }
      processMessageNoCompression(node, msg, msgData)
    }
  );
}
module.exports = function (RED) {
  function KafkaProducerNode(n) {
    RED.nodes.createNode(this, n)
    const node = Object.assign(this, n, {
      connected: false,
      nodeQSize: 1000,
      retryCount: 0,
      successes: 0,
      deadletters: 0,
      errorDiscarded: 0,
      compressionError: 0,
      waiting: []
    })
    node.sendDeadletter = sendDeadletter.bind(this);
    node.brokerNode = RED.nodes.getNode(node.broker)
    setStatus(node, 'Initialising', 'yellow')
    try {
      if(node.compressionType==null ){
        switch(node.attributes){ // old method conversion
         case 1: 
             node.compressionType="setGzip";
             node.attributes=0;
             break;
         case 2:arguments
             node.compressionType="setSnappy";
             node.attributes=0;
             break;
         default:
             node.compressionType="none";
        } 
     }

      if (!node.hasOwnProperty('key-type')) node['key-type'] = 'str'
      node.getKey = node.key ? evalInFunction(node, 'key') : () => undefined
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      if (!kafka) kafka = node.brokerNode.getKafkaDriver();
      if(node.compressionType==null || node.compressionType=="none") {
        node.processMessage=processMessageNoCompression.bind(node)
      } else {
        node.compressor=new compressionTool(),
        node.compressor[node.compressionType];
        node.processMessage=processMessageCompression.bind(node)
      }
      if (node.convertFromJson) {
        node.getMessage = (RED, node, msg) => JSON.stringify(msg.payload);
      } else {
        node.getMessage = (RED, node, msg) => {
          const data = msg.payload;
          if (data == null) return null
          const dataType = typeof data;
          if (['string', 'number'].includes(dataType)
            || data instanceof Buffer) return msg.payload;
          if (dataType == 'object') return JSON.stringify(msg.payload);
          if (data.buffer)
            return Buffer.from(data.buffer)
          throw Error("unknow data type: " + dataType)
        }
      }
      node.brokerNode.onStateUp.push({
        node: node,
        callback: function () {
          connect(node)
        }
      }) // needed due to bug in kafka driver
    } catch (ex) {
      node.error(ex.message)
      logger.sendErrorAndStackDump(ex.message, ex)
      setStatus(node, ex.message, 'red')
      return
    }
    node.queueMsg = function (msg, retry) {
      if (!node.waiting.length) setStatus(node, 'Connection down started queuing messages', "yellow")
      if (retry) {
        node.waiting.unshift(msg)
      } else {
        if (node.waiting.length > node.nodeQSize)
          setStatus(node, 'Connection down, max queue depth reached ' + node.waiting.length, "red")
        else
          node.waiting.push(msg)
      }
      if (!(node.waiting.length % 100)) {
        setStatus(node, 'Connection down, queue depth reached ' + node.waiting.length, "yellow")
      }
    }
    if (logger.active) logger.send({ label: 'kafkaProducer', node: node.id, getKey: node.getKey.toString() })
    node.on('input', function (msg) {
      try {
        node.processMessage(node,node.getMessage(RED, node, msg));
      } catch (ex) {
        if (logger.active) logger.sendErrorAndStackDump(ex.message, ex)
        node.error('input error:' + ex.message)
      }
    })
    node.on('close', function (removed, done) {
      setStatus(node, "closed", "red")
      node.producer.close(false, () => {
        node.log('closed')
      })
      done()
    })
  }
  RED.nodes.registerType(logger.label, KafkaProducerNode)
  setupHttpAdmin(RED, logger.label, {
    Status: (RED, node, callback) => callback({
      connecting: node.connecting,
      connected: node.connected,
      retryCount: node.retryCount,
      "no. waiting": node.waiting.length,
      successes: node.successes,
      deadletters: node.deadletters,
      errorDiscarded: node.errorDiscarded,
    }),
    "Connect": (RED, node, callback) => {
      try {
        connect(node);
        callback("connect issued");
      } catch (ex) {
        callback(ex.message);
      }
    },
    "Close": (RED, node, callback) => {
      try {
        logger.warn("httpadmin close issued")
        node.producer.close(function (err, data) {
          if (logger.active) logger.send({ label: 'kafkaProducer httpadmin close callback', node: node.id, error: err, data: data })
          callback(err || "Closed")
        });
      } catch (ex) {
        if (node.producer.close) {
          callback(ex.message);
        } else {
          callback("not connected")
        }
        callback(ex.message);
      }
    },
    "Retry Q": (RED, node, callback) => {
      if (node.waiting.length) {
        producerSend(node, undefined, 1)
        callback("Retry issued");
      } else {
        callback("Empty Queue");
      }
    },
    "Reset Status": (RED, node, callback) => {
      showStatus(node);
      callback("Status reset");
    },
    "Clear Queue": (RED, node, callback) => {
      node.waiting = [];
      callback("Queue cleared")
    },
    "refreshMetadata": (RED, node, callback) => {
      try {
        logger.warn("httpadmin refreshMetadata issued")
        if(node.connected!==true) throw Error("not connected");
        if(node.client.refreshMetadata==null) throw Error("no refreshMetadata");
        node.client.refreshMetadata([node.topic], (err) => {
          if (logger.active) logger.send({ label: 'kafkaProducer httpadmin refreshMetadata callback', node: node.id, error: err})
          callback(err || "Refreshed")
          if (node.waiting) producerSend(node);
        });
      } catch (ex) {
        if (node.producer.close) {
          callback(ex.message);
        } else {
          callback("not connected")
        }
      }
    },
  })
}