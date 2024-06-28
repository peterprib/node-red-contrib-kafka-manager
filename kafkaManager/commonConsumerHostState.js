const nodeStatus = require('./nodeStatus.js')
function commonConsumerHostState(node,logger) {
    node.status({ fill: 'red', shape: 'ring', text: 'broker down' })
    const brokerNode = node.brokerNode;
    //  const client=brokerNode.client
    const client = brokerNode.getClient()
    node.client = client
    const hostState = brokerNode.hostState
    hostState.onUp(next => {
        logger.active && logger.send({ label: 'brokerNode.hostState.onUp', node: { id: node.id, name: node.name } })
        nodeStatus(node, { client: "Client connecting" })
        client.setUp((next) => {
            logger.active && logger.send({ label: 'brokerNode.hostState.onUp client.setUp', node: { id: node.id, name: node.name } })
            nodeStatus(node)
            next()
        })
        next()
    }).onDown(next => {
        logger.active && logger.send({ label: 'brokerNode.hostState.onDown', node: { id: node.id, name: node.name } })
        nodeStatus(node)
        client.forceDown(nextForceDown => {
            logger.active && logger.send({ label: 'brokerNode.hostState.onDown client.forceDown', node: { id: node.id, name: node.name } })
            nodeStatus(node)
            nextForceDown()
        })
        next()
    }).beforeDown(next => {
        logger.active && logger.send({ label: 'brokerNode.hostState.beforeDown', node: { id: node.id, name: node.name } })
        nodeStatus(node)
        client.setDown(next => {
            logger.active && logger.send({ label: 'brokerNode.hostState.beforeDown setDown', node: { id: node.id, name: node.name } })
            nodeStatus(node)
            next()
        })
        next()
    })

    client.onUp(next => {
        logger.active && logger.send({ label: 'client.onUp', node: node.id, name: node.name })
        nodeStatus(node, { client: "Connecting" })
        node.setUp(next => {
            logger.active && logger.send({ label: 'client.onUp setUp', node: node.id, name: node.name })
            nodeStatus(node)
            next()
        })
        next()
    }).onDown(next => {
        logger.active && logger.send({ label: 'client.onDown', node: node.id, name: node.name })
        nodeStatus(node)
        if (node.isAvailable()) {
            node.status({ fill: 'red', shape: 'ring', text: 'forcing down connection' })
            node.forceDown(nextForceDown => {
                logger.active && logger.send({ label: 'client.onDown forceDown', node: node.id, name: node.name })
                nodeStatus(node)
                nextForceDown()
            })
        }
        next()
    })
    node.pause = (done) => {
        logger.active && logger.send({ label: 'pause', node: node.id, name: node.name })
        if (node.consumer) {
            node.paused = true
            node.consumer.pause()
            node.status({ fill: 'red', shape: 'ring', text: 'Paused' })
        } else nodeStatus(node)
        done && done()
    }
    node.resume = (done) => {
        logger.active && logger.send({ label: 'resume', node: node.id, name: node.name })
        if (node.consumer) {
            node.resumed = true
            node.consumer.resume()
            node.status({ fill: 'green', shape: 'ring', text: 'Ready' })
        } else nodeStatus(node)
        done && done()
    }
    node.commit = (done) => {
        logger.active && logger.send({ label: 'commit', node: node.id, name: node.name })
        if (node.consumer)
            node.consumer.commit((err, data) => {
                logger.active && logger.send({ label: 'commit', node: node.id, name: node.name, error: err, data: data })
                done && done(data, err)
            })
        else done && done(null, "connection down")
    }
    node.state
        .onUp(next => {
            logger.active && logger.send({ label: 'onUp', node: { id: node.id, name: node.name } })
            if (node.paused) {
                node.log('state changed to up and in paused state')
                node.paused()
            } else {
                node.log('state changed to up, resume issued')
                node.resume()
            }
            nodeStatus(node)
            next()
        }).onDown(next => {
            logger.active && logger.send({ label: 'onDown', node: { id: node.id, name: node.name } })
            nodeStatus(node)
            next()
        })
    node.on('close', function (removed, done) {
        logger.active && logger.send({ label: 'close', node: node.id, name: node.name })
        node.setDown((next) => {
            logger.active && logger.send({ label: 'close setDown', node: node.id, name: node.name })
            done && done()
            next && next()
        })
    })

}
module.exports = commonConsumerHostState