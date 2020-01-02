##Learning Kafka

This project contains sample code for below concepts

- Various Producers (Simple, Custom, Bytes, Json, Avro, Proto)
- Various Consumers (Simple, Custom, Bytes, Json, Avro, Proto)
- Various Serializers
- Various Deserializers
- Custom Partitioner
- Utility class to know the depth of a topic

####Set up

- Extract the source code and run
```$shell
mvn clean install
```
This will generate class files needed for the project, especially for proto classes

- Start Zookeeper
```$xslt
cd $KAFKA_HOME
bin/zookeeper-server-start.sh config/zookeeper.properties
```
NB - Set KAFKA_HOME to the kafka installation directory

- Start Broker
Sometimes one broker is enough, but for some code, multiple broker is needed.
```$xslt
cd $KAFKA_HOME
bin/kafka-server-start.sh config/server.properties
```

- Create a topic
```$xslt
cd $KAFKA_HOME
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions part-num --topic topic-name
```

- Then you can run publisher and then consumer
- See the output in console