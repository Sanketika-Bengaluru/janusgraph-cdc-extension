package org.sunbird.janusgraph.cdc;

public class KafkaEventSink implements EventSink {

    private final KafkaEventProducer producer;

    public KafkaEventSink(String bootstrapServers, String topic) {
        this.producer = new KafkaEventProducer(bootstrapServers, topic);
    }

    @Override
    public void send(String key, String message) {
        this.producer.send(key, message);
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
