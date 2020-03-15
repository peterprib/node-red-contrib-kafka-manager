# [node-red-contrib-kafka-manager][2]
[Node Red][1] for working with apache kafka, a streaming product.
First initial release using [kafka-node][4] .

* Kafka Broker
* Kafka Admin
* Kafka Consumer
* Kafka ConsumerGroup
* Kafka Offset
* Kafka Producer

Has a test GUI which allows topics to be added.

Note: all nodes run in debug mode for 100 messages then turns off.

------------------------------------------------------------

## Kafka Broker

Defines the client interface to kafka. 

![Kafka Broker](documentation/broker.JPG "Kafka Broker")
![Kafka Broker Options](documentation/brokerOptions.JPG "Kafka Broker Options")

------------------------------------------------------------

## Kafka Admin

Provide the ability to process administration tasks such as create and list topic. 

![Admin](documentation/admin.JPG "Admin")

------------------------------------------------------------

## Kafka Consumer

Consumer of topic messages in kafka which are generated into node-red message. 
Provides types of base and high level.

![Kafka Consumer](documentation/consumer.JPG "Kafka Consumer")
![Kafka Consumer Options](documentation/consumerOptions.JPG "Kafka Consumer Options")
![Kafka Consumer Fetch](documentation/consumerFetch.JPG "Kafka Consumer Fetch")
![Kafka Consumer Encoding](documentation/consumerEncoding.JPG "Kafka Consumer Encoding")
------------------------------------------------------------

## Kafka Consumer Group

Consumer of topic messages in kafka which are generated into node-red message. 

![Kafka Consumer Group](documentation/consumerGroup.JPG "Kafka Consumer Group")
![Kafka Consumer Group Options](documentation/consumerGroupOptions.JPG "Kafka Consumer Options Group")

------------------------------------------------------------

## Kafka Offset

Get various offsets from Kafka. Which type are set via msg.action or msg.topic.  msg.payload states the types of options.



![Kafka Offset](documentation/offset.JPG "Kafka Offset")

------------------------------------------------------------

## Kafka Producer

Converts a node-red message into a kafka messages.
Provides types of base and high level.

![Kafka Producer](documentation/producer.JPG "Kafka Producer")

------------------------------------------------------------

## Simple Web Admin Panel

Simple Web page monitor and admin panel 

![Web](documentation/webAdmin.JPG "Web")

Producing flow can be found in test flow

![Web Flow](documentation/gui.JPG "Web Flow")


------------------------------------------------------------

# Install

Run the following command in the root directory of your Node-RED install or via GUI install

    npm install node-red-contrib-kafka-manager


# Tests

Test/example flow in test/generalTest.json

![Tests](documentation/tests.JPG "Tests")

Includes sample script for start kafka in windows using node-red

![Scripts](documentation/scripts.JPG "Scripts")

------------------------------------------------------------

# Version
0.2.14 Add self serve TLS and fix bug plus mask ssl info when debug logging
0.2.9 Change debugging mechanism and add kafka-node to dependencies
0.2.8 Added all admin api's per Kafka 2.3 but dependent on [kafka-node][4] update.
Remove refresh metadata, automated if problem.  Fix consumer group errors.  Add tests for admin calls.
0.2.7 If offsetOutOfRange pause consumer.  Added in deleteTopics but dependant on [kafka-node][4] update.
0.2.6 More fixes for error processing on invalid topic
0.2.4 Fix for error processing
0.2.3 Fix for multi nodes on broker

0.2.2 Stopped bug where producer on connection initiates a null message. Fix bug with restart logic on fail and order of messages on failed retry

0.2.1 When messages being queued as producer is waiting on connection or reconnection show producer in problem state.

0.2.0 Add nodes consumer group and offset.
	Another fix to issue with initial no kafka.
	describe groups implemented

0.1.0 Add in High level producer/consumer.
  	Further fixes to make connection more robust on kafka up/down
  	Multi host per broker
  	Multi Topic for consumer

0.0.2 Add in High level producer. 
  	Make connection more robust on kafka up/down due to bugs and problem points in in [kafka-node][4] 

0.0.1 base

# Author

[Peter Prib][3]

[1]: http://nodered.org "node-red home page"

[2]: https://www.npmjs.com/package/node-red-contrib-kafka-manager "source code"

[3]: https://github.com/peterprib "base github"

[4]: https://github.com/SOHU-Co/kafka-node "npm kafka-node"
