package org.sunbird.janusgraph.cdc;

public interface EventSink {
    void send(String key, String message);

    void close();
}
