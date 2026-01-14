package org.sunbird.janusgraph.cdc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A lightweight wrapper around KafkaProducer.
 */
public class KafkaEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventProducer.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaEventProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "janusgraph-cdc-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Optimize for throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        this.producer = new KafkaProducer<>(props);
        logger.info("KafkaEventProducer initialized for topic: {}", topic);
    }

    public void send(String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message to Kafka. Key: {}", key, exception);
                } else {
                    logger.debug("Message sent to Kafka. Topic: {}, Partition: {}, Offset: {}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Error sending message to Kafka", e);
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
