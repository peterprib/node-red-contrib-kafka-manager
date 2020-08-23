const logger = new (require("node-red-contrib-logger"))("Kafka Rollback");
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

module.exports = function (RED) {
	function KafkaRollbackNode (n) {
		RED.nodes.createNode(this, n)
		let node = Object.assign(this, n);
		node.on('input', function (msg) {
			if(msg._kafka) {
				node.warn("kakfa rollback closing consumer")
				msg._kafka.rollback(
					()=>{
						node.send(msg);
					},
					(ex)=>{
						node.send([null,msg]);
					}
				);
			} else {
				node.send(msg);
			}
		});
	}
	RED.nodes.registerType(logger.label, KafkaRollbackNode);
}