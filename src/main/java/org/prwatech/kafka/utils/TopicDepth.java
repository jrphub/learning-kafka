package org.prwatech.kafka.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class TopicDepth {
    private static final Logger logger = LoggerFactory.getLogger(TopicDepth.class);

    private static final String TOPIC="employee-json-serde";
    private static final Object BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        /*props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "depthConsumer");*/
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);
        // Subscribe to the topic.
        //consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    public static void main(String[] args) {
        Consumer<String, String> consumer = createConsumer();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(TOPIC);

        List<TopicPartition> topicPartitions = partitionInfoList.stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
        
        logger.info("TopicPartitions : {}", topicPartitions);
        consumer.assign(topicPartitions);

        Map<TopicPartition, Long> startPartitionMap = consumer.beginningOffsets(topicPartitions);
        
        logger.info("Showing beginning offset...");
        startPartitionMap.forEach((k,v) -> logger.info(k + ":" + v));

        Map<TopicPartition, Long> endPartitionMap = consumer.endOffsets(topicPartitions);

        logger.info("Showing end offset ...");
        endPartitionMap.forEach((k, v) -> logger.info(k + ":" + v));

        Map<TopicPartition, Long> depthMap = new HashMap<>();
        endPartitionMap.forEach((k, v) -> {
            long startOffset = startPartitionMap.get(k);
            long endOffset = v;
            depthMap.put(k, endOffset - startOffset);
        });

        logger.info("Showing depth offset ...");
        depthMap.forEach((k,v) -> logger.info(k + ":" + v));

        consumer.close();
        

    }

}
