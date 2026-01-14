package org.sunbird.janusgraph.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileEventSink implements EventSink {

    private static final Logger logger = LoggerFactory.getLogger(LogFileEventSink.class);

    @Override
    public void send(String key, String message) {
        logger.info("GraphLogEvent: key={}, message={}", key, message);
    }

    @Override
    public void close() {
        // No-op for logger
    }
}
