package org.sunbird.janusgraph.cdc;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TelemetryMessageConverter implements MessageConverter {

    private static final Logger logger = LoggerFactory.getLogger(TelemetryMessageConverter.class);

    @Override
    public Map<String, Object> convert(JanusGraphVertex vertex, ChangeState changeState, String operationType,
            TransactionId txId) {
        long ets = System.currentTimeMillis();
        String mid = "LP." + ets + "." + UUID.randomUUID();

        Map<String, Object> telemetry = new HashMap<>();
        // Fixed context values as per typical Sunbird telemetry, customized for CDC
        telemetry.put("eid", "BE_GRAPH_LOG"); // Using a generic event ID for graph logs
        telemetry.put("ets", ets);
        telemetry.put("mid", mid);
        telemetry.put("ver", "3.0"); // Telemetry v3

        Map<String, Object> pdata = new HashMap<>();
        pdata.put("id", "org.sunbird.janusgraph.cdc");
        pdata.put("ver", "1.0");
        pdata.put("pid", "GraphLogProcessor");
        telemetry.put("pdata", pdata);

        // edata contains the actual transaction details
        Map<String, Object> edata = new HashMap<>();
        edata.put("nodeUniqueId", vertex.id().toString());
        edata.put("operationType", operationType);
        edata.put("txId", txId.toString());
        edata.put("objectType", vertex.label());
        edata.put("graphId", "domain"); // Defaulting to domain

        if ("CREATE".equals(operationType)) {
            Map<String, Object> properties = new HashMap<>();
            try {
                vertex.properties().forEachRemaining(p -> {
                    properties.put(p.key(), p.value());
                });
                edata.put("properties", properties);

                // Use functional ID if available
                if (properties.containsKey("IL_UNIQUE_ID")) {
                    edata.put("nodeUniqueId", properties.get("IL_UNIQUE_ID"));
                }
            } catch (Exception e) {
                logger.warn("Failed to extract properties for vertex {}", vertex.id(), e);
            }
        }

        telemetry.put("edata", edata);

        return telemetry;
    }
}
