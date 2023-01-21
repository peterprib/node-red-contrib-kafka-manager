const logger = new (require('node-red-contrib-logger'))('Kafka Commit')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')

module.exports = function (RED) {
  function KafkaCommitNode (n) {
    RED.nodes.createNode(this, n)
    const node = Object.assign(this, n)
    node.on('input', function (msg) {
      if (msg._kafka) {
        msg._kafka.commit(
          () => {
            node.send(msg)
          },
          (error) => {
            node.send([null, msg])
          }
        )
      } else {
        node.send(msg)
      }
    })
  }
  RED.nodes.registerType(logger.label, KafkaCommitNode)
}
