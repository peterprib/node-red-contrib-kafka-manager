//const assert=require('assert');
const should = require("should");
const helper = require("node-red-node-test-helper");
const kafkaAdmin = require("../kafkaManager/kafkaAdmin.js");
const kafkaBroker = require("../kafkaManager/kafkaBroker.js");
const kafkaCommit = require("../kafkaManager/kafkaCommit.js");
const kafkaConsumer = require("../kafkaManager/kafkaConsumer.js");
const kafkaConsumerGroup = require("../kafkaManager/kafkaConsumerGroup.js");
const kafkaOffset = require("../kafkaManager/kafkaOffset.js");
const kafkaProducer = require("../kafkaManager/kafkaProducer.js");
const kafkaRollback = require("../kafkaManager/kafkaRollback.js");
const nodes=[kafkaBroker,kafkaAdmin,kafkaCommit,kafkaConsumer,kafkaConsumerGroup,kafkaOffset,kafkaProducer,kafkaRollback]

helper.init(require.resolve('node-red'));

function getAndTestNodeProperties(o) {
	const n = helper.getNode(o.id);
	if(n==null) throw Error("can find node "+o.id);
	for(let p in o) n.should.have.property(p, o[p]);
	return n;
}

const broker={
	"id" : "brokerID",
	"type" : "Kafka Broker",
	"name" : "Kafta",
	"hosts" : [ {
		"host" : "127.0.0.1",
		"port" : 9092
	} ],
	"hostsEnvVar" : "",
	"connectTimeout" : "10000",
	"requestTimeout" : "30000",
	"autoConnect" : "true",
	"idleConnection" : "5",
	"reconnectOnIdle" : "true",
	"maxAsyncRequests" : "10",
	"checkInterval" : "10",
	"selfSign" : true,
	"usetls" : false,
	"useCredentials" : false
} ;

const admin={
	"id" : "kafkaAdminId",
	"type" : "Kafka Admin",
	"name" : "Kafka Admin name",
	"broker" : broker.id
};
const consumer_test={
	"id" : "consumerID",
	"type" : "Kafka Consumer",
	"name" : "Kafka Consumer Name",
	"broker" : broker.id,
	"topic" : null,
	"topics" : [ {
		"topic" : "test",
		"offset" : 0,
		"partition" : 0
	}, {
		"topic" : "atest",
		"offset" : 0,
		"partition" : 0
	} ],
	"groupId" : "kafka-node-group",
	"autoCommit" : "true",
	"autoCommitIntervalMs" : 5000,
	"fetchMaxWaitMs" : 100,
	"fetchMinBytes" : 1,
	"fetchMaxBytes" : 1048576,
	"fromOffset" : 0,
	"encoding" : "utf8",
	"keyEncoding" : "utf8",
	"connectionType" : "Consumer"
};
const producer={
	"id" : "producerId",
	"type" : "Kafka Producer",
	"name" : "Kafka Producer Name",
	"broker" : broker.id,
	"topic" : "test",
	"requireAcks" : 1,
	"ackTimeoutMs" : 100,
	"partitionerType" : 0,
	"key" : "",
	"partition" : 0,
	"attributes" : 0,
	"connectionType" : "Producer"
};
const producerHL={
	"id" : "producerHLId",
	"type" : "Kafka Producer",
	"name" : "Kafka Producer HL Name",
	"broker" : broker.id,
	"topic" : "atest",
	"requireAcks" : 1,
	"ackTimeoutMs" : 100,
	"partitionerType" : 0,
	"key" : "",
	"partition" : 0,
	"attributes" : 0,
	"connectionType" : "HighLevelProducer"
};

const consumer_atest={
	"id" : "consumerId",
	"type" : "Kafka Consumer",
	"name" : "Consumer topic atest",
	"broker" : broker.id,
	"topics" : [ {
		"topic" : "atest",
		"offset" : 0,
		"partition" : 0
	} ],
	"groupId" : "groupTopicAtest",
	"autoCommit" : "true",
	"autoCommitIntervalMs" : 5000,
	"fetchMaxWaitMs" : 100,
	"fetchMinBytes" : 1,
	"fetchMaxBytes" : 1048576,
	"fromOffset" : 0,
	"encoding" : "utf8",
	"keyEncoding" : "utf8",
	"connectionType" : "Consumer"
};

const consumerGroup={
	"id" : "consumerGroupID",
	"type" : "Kafka Consumer Group",
	"name" : "consumerGroup",
	"broker" : broker.id,
	"groupId" : "aGroup",
	"sessionTimeout" : 15000,
	"protocol" : [ "roundrobin" ],
	"encoding" : "utf8",
	"fromOffset" : "latest",
	"commitOffsetsOnFirstJoin" : "true",
	"outOfRangeOffset" : "earliest",
	"topics" : [ "test", "topic2" ]
};

const createTopics={
	"topic" : "createTopics",
	"payload" : "[{\"topic\":\"aTestRemoveTopic\",\"partitions\":1,\"replicationFactor\":1},{\"topic\":\"aTestRemoveTopicfail\"},{\"topic\":\"test\",\"partitions\":1,\"replicationFactor\":1},{\"topic\":\"atest\",\"partitions\":1,\"replicationFactor\":1},{\"topic\":\"testCommit\",\"partitions\":1,\"replicationFactor\":1},{\"topic\":\"testRollback\",\"partitions\":1,\"replicationFactor\":1}]",
	"wires" : [ [ "31c34a4.2603ab6" ] ]
};

function testFlow(done,data,result) {
	const flow = [
		broker,
		admin,
		producer,
		producerHL,
		Object.assign(consumer_test,{wires : [ [ "outHelper" ],["errorHelper"] ]}),
		Object.assign(consumer_atest,{wires : [ [ "outHelper" ],["errorHelper"] ]}),
		consumerGroup,
		{id :"outHelper",	type : "helper"},
		{id :"errorHelper",	type : "helper"}
	];
	helper.load(nodes, flow,function() {
		const brokerNode=getAndTestNodeProperties(broker);
		const adminNode=getAndTestNodeProperties(admin);
		const producerNode=getAndTestNodeProperties(producer);
		const producerHLNode=getAndTestNodeProperties(producerHL);
		const consumer_testNode=getAndTestNodeProperties(consumer_test);
		const consumer_atestNode=getAndTestNodeProperties(consumer_atest);
		const consumerGroupNode=getAndTestNodeProperties(consumerGroup);
		const outHelper = helper.getNode("outHelper");
		const errorHelper = helper.getNode("errorHelper");
		outHelper.on("input", function(msg) {
			done();
		});
		errorHelper.on("input", function(msg) {
			done("error  check log output");
		});
		adminNode.receive(createTopics);
		done();
	});
}

describe('basic test', function() {
	beforeEach(function(done) {
		helper.startServer(done);
	});
	afterEach(function(done) {
		helper.unload();
		helper.stopServer(done);
	});
	it('load objects', function(done) {
		testFlow(done);
	});
});

