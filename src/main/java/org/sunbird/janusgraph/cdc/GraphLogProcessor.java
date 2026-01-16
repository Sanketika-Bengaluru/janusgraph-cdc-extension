package org.sunbird.janusgraph.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * GraphLogProcessor running inside JanusGraph Server.
 * Listens to "learning_graph_events" user log and publishes changes to
 * configured sinks.
 */
public class GraphLogProcessor {

    private static final Logger logger = LoggerFactory.getLogger(GraphLogProcessor.class);
    private static final String LOG_IDENTIFIER = "learning_graph_events";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Configurable components
    private static List<EventSink> sinks = new ArrayList<>();
    private static MessageConverter converter;
    private static boolean isStarted = false;

    public static synchronized void start(JanusGraph graph, Map<String, Object> config) {
        if (isStarted) {
            logger.warn("GraphLogProcessor is already running.");
            return;
        }

        // Configuration passed from script
        boolean enableProcessor = Boolean
                .parseBoolean(String.valueOf(config.getOrDefault("graph.txn.log_processor.enable", "false")));
        if (!enableProcessor) {
            logger.info("GraphLogProcessor is disabled by config.");
            return;
        }

        logger.info("Starting GraphLogProcessor...");

        // INitialize Converter
        String converterType = (String) config.getOrDefault("graph.txn.log_processor.converter", "DEFAULT");
        if ("TELEMETRY".equalsIgnoreCase(converterType)) {
            converter = new TelemetryMessageConverter();
            logger.info("Using TelemetryMessageConverter");
        } else if ("SUNBIRD_LEGACY".equalsIgnoreCase(converterType)) {
            converter = new SunbirdLegacyMessageConverter();
            logger.info("Using SunbirdLegacyMessageConverter");
        } else {
            converter = new SimpleMessageConverter();
            logger.info("Using SimpleMessageConverter");
        }

        // Initialize Sinks
        sinks.clear();
        String sinksConfig = (String) config.getOrDefault("graph.txn.log_processor.sinks", "KAFKA"); // Default to KAFKA
        String[] sinkTypes = sinksConfig.split(",");

        for (String sinkType : sinkTypes) {
            if ("KAFKA".equalsIgnoreCase(sinkType.trim())) {
                String kafkaServers = (String) config.getOrDefault("kafka.bootstrap.servers", "localhost:9092");
                String kafkaTopic = (String) config.getOrDefault("kafka.topics.graph.event",
                        "sunbirddev.learning.graph.events");
                sinks.add(new KafkaEventSink(kafkaServers, kafkaTopic));
                logger.info("Added Kafka Event Sink (Topic: {})", kafkaTopic);
            } else if ("LOG".equalsIgnoreCase(sinkType.trim())) {
                sinks.add(new LogFileEventSink());
                logger.info("Added Log File Event Sink");
            }
        }

        if (sinks.isEmpty()) {
            logger.warn("No sinks configured. Processor will consume logs but output nowhere.");
        }

        try {
            LogProcessorFramework framework = JanusGraphFactory.openTransactionLog(graph);
            framework.addLogProcessor(LOG_IDENTIFIER)
                    .setProcessorIdentifier("janusgraph-cdc-processor")
                    .setStartTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                    .addProcessor(new ChangeProcessor() {
                        @Override
                        public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
                            try {
                                processChanges(txId, changeState);
                            } catch (Exception e) {
                                logger.error("Error processing transaction logs", e);
                            }
                        }
                    })
                    .build();

            isStarted = true;
            logger.info("GraphLogProcessor started successfully with {} sinks.", sinks.size());

        } catch (Exception e) {
            logger.error("Failed to start GraphLogProcessor", e);
        }
    }

    private static void processChanges(TransactionId txId, ChangeState changeState) {
        Set<Object> processedIds = new HashSet<>();

        // 1. Process Added Vertices (CREATE)
        for (JanusGraphVertex vertex : changeState.getVertices(Change.ADDED)) {
            processVertexChange(vertex, changeState, "CREATE", txId, null);
            processedIds.add(vertex.id());
        }

        // 2. Process Removed Vertices (DELETE)
        for (JanusGraphVertex vertex : changeState.getVertices(Change.REMOVED)) {
            processVertexChange(vertex, changeState, "DELETE", txId, null);
            processedIds.add(vertex.id());
        }

        // 3. Process Property Updates on Existing Vertices (UPDATE)
        // JanusGraph registers property updates as REMOVED (old val) and ADDED (new
        // val) on the same vertex
        Set<JanusGraphVertex> changedVertices = changeState.getVertices(Change.ANY);
        for (JanusGraphVertex vertex : changedVertices) {
            if (!processedIds.contains(vertex.id())) {
                // Determine if there are actual property diffs
                Map<String, Map<String, Object>> propertyDiffs = getPropertyDiffs(vertex, changeState);
                if (!propertyDiffs.isEmpty()) {
                    processVertexChange(vertex, changeState, "UPDATE", txId, propertyDiffs);
                }
            }
        }
    }

    private static Map<String, Map<String, Object>> getPropertyDiffs(JanusGraphVertex vertex, ChangeState changeState) {
        Map<String, Map<String, Object>> diffs = new HashMap<>();

        // Capture Removed Properties (Old Values)
        // usage: getProperties(vertex, change, keys...) - empty keys means all
        Iterator<JanusGraphVertexProperty> removedProps = changeState
                .getProperties(vertex, Change.REMOVED).iterator();
        while (removedProps.hasNext()) {
            JanusGraphVertexProperty p = removedProps.next();
            String key = p.key();
            // Filter system properties if needed
            if (!isSystemProperty(key)) {
                diffs.putIfAbsent(key, new HashMap<>());
                diffs.get(key).put("ov", p.value());
            }
        }

        // Capture Added Properties (New Values)
        Iterator<JanusGraphVertexProperty> addedProps = changeState.getProperties(vertex, Change.ADDED)
                .iterator();
        while (addedProps.hasNext()) {
            JanusGraphVertexProperty p = addedProps.next();
            String key = p.key();
            if (!isSystemProperty(key)) {
                diffs.putIfAbsent(key, new HashMap<>());
                diffs.get(key).put("nv", p.value());
            }
        }

        return diffs;
    }

    private static boolean isSystemProperty(String key) {
        // Add any system property filters here
        return false;
    }

    private static void processVertexChange(JanusGraphVertex vertex, ChangeState changeState, String operationType,
            TransactionId txId, Map<String, Map<String, Object>> propertyDiffs) {
        try {
            // Convert message using the Strategy Pattern, but pass diffs for UPDATE
            Map<String, Object> event;

            // For UPDATE, we manually construct the event to include diffs
            // Ideally, we should refactor Converter interface to accept diffs,
            // but for now we patch it here to ensure 'ov' and 'nv' are present.
            if ("UPDATE".equals(operationType) && propertyDiffs != null) {
                event = new HashMap<>();
                event.put("operationType", "UPDATE");
                event.put("nodeGraphId", "domain"); // Default, should be dynamic if possible
                event.put("nodeUniqueId", getUniqueId(vertex, changeState));
                event.put("objectType", vertex.label());
                event.put("timestamp", System.currentTimeMillis());
                event.put("txId", txId.toString());

                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("properties", propertyDiffs);
                event.put("transactionData", transactionData);
            } else {
                // Fallback to existing converter for CREATE/DELETE
                event = converter.convert(vertex, changeState, operationType, txId);
            }

            String json = mapper.writeValueAsString(event);
            String key = vertex.id().toString();

            // Send to all sinks
            for (EventSink sink : sinks) {
                try {
                    sink.send(key, json);
                } catch (Exception e) {
                    logger.error("Error sending event to sink: {}", sink.getClass().getSimpleName(), e);
                }
            }
            logger.debug("Processed event: {}", json);

        } catch (Exception e) {
            logger.error("Error converting/processing vertex change event", e);
        }
    }

    private static String getUniqueId(JanusGraphVertex vertex, ChangeState changeState) {
        // Try to get IL_UNIQUE_ID
        try {
            if (vertex.property("IL_UNIQUE_ID").isPresent()) {
                return (String) vertex.value("IL_UNIQUE_ID");
            }
            // Fallback: check if it was just added in this transaction
            Iterator<JanusGraphVertexProperty> props = changeState.getProperties(vertex, Change.ADDED)
                    .iterator();
            while (props.hasNext()) {
                JanusGraphVertexProperty p = props.next();
                if ("IL_UNIQUE_ID".equals(p.key())) {
                    return (String) p.value();
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return vertex.id().toString();
    }

    public static synchronized void shutdown() {
        for (EventSink sink : sinks) {
            try {
                sink.close();
            } catch (Exception e) {
                logger.warn("Error closing sink", e);
            }
        }
        sinks.clear();
        isStarted = false;
        logger.info("GraphLogProcessor stopped.");
    }
}
