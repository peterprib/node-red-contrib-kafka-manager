function nodeStatus(node, states) {
    try {
        const labels = Object.assign({ connection: "ready", client: "client connected", noClient: "host available", host: "host unavailable" }, states)
        if (node.brokerNode.hostState == null || node.brokerNode.hostState.isNotAvailable()) {
            node.status({ fill: 'red', shape: 'ring', text: labels.host })
            return
        }
        if (node.client == null || node.client.isNotAvailable()) {
            node.status({ fill: 'yellow', shape: 'ring', text: labels.noClient })
            return
        }
        if (node.isAvailable()) {
            node.status({ fill: 'green', shape: 'ring', text: labels.connection })
            return
        }
        node.status({ fill: 'yellow', shape: 'ring', text: labels.client })
    } catch (ex) {
        console.error(ex.stack)
        node.status({ fill: 'red', shape: 'ring', text: "check log for error" })
    }
}
module.exports = nodeStatus