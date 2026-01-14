package org.sunbird.janusgraph.cdc;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SimpleMessageConverter implements MessageConverter {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMessageConverter.class);

    @Override
    public Map<String, Object> convert(JanusGraphVertex vertex, String operationType, TransactionId txId) {
        Map<String, Object> event = new HashMap<>();
        event.put("nodeGraphId", "domain");
        event.put("nodeUniqueId", vertex.id().toString());
        event.put("operationType", operationType);
        event.put("timestamp", System.currentTimeMillis());
        event.put("txId", txId.toString());

        // Extract Label
        event.put("objectType", vertex.label());

        // Extract Properties
        // For CREATE, we want full snapshot.
        if ("CREATE".equals(operationType)) {
            Map<String, Object> properties = new HashMap<>();
            try {
                vertex.properties().forEachRemaining(p -> {
                    properties.put(p.key(), p.value());
                });
                event.put("properties", properties);

                // Try to find a better unique ID if available
                if (properties.containsKey("IL_UNIQUE_ID")) {
                    event.put("nodeUniqueId", properties.get("IL_UNIQUE_ID"));
                }
            } catch (Exception e) {
                logger.warn("Failed to extract properties for vertex {}", vertex.id(), e);
            }
        }
        return event;
    }
}
