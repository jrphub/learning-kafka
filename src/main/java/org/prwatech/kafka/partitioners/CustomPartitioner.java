package org.prwatech.kafka.partitioners;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class CustomPartitioner extends DefaultPartitioner {
    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);
    @Override
    public int partition(String topic,
                         Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes,
                         Cluster cluster) {
        String partitionKey = null;
        if (Objects.nonNull(key)) {
            partitionKey = (String) key;
            partitionKey = partitionKey.substring(0,2);
            logger.info("parition key :" + partitionKey);
            keyBytes = partitionKey.getBytes();
        }
        return super.partition(topic,
                partitionKey, keyBytes,
                value, valueBytes,
                cluster);

    }
}
