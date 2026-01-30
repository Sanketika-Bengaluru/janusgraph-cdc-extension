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

    // Singleton instance
    private static volatile GraphLogProcessor instance;

    // Instance State
    private List<EventSink> sinks = new ArrayList<>();
    private MessageConverter converter;
    private boolean isStarted = false;

    private GraphLogProcessor() {
        // Prevent direct instantiation
    }

    public static synchronized void start(JanusGraph graph, Map<String, Object> config) {
        if (instance == null) {
            instance = new GraphLogProcessor();
        }
        instance.init(graph, config);
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.stop();
            instance = null;
        }
    }

    private void init(JanusGraph graph, Map<String, Object> config) {
        if (isStarted) {
            logger.info("GraphLogProcessor is already running.");
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

    private void stop() {
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

    private void processChanges(TransactionId txId, ChangeState changeState) {
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

    private Map<String, Map<String, Object>> getPropertyDiffs(JanusGraphVertex vertex, ChangeState changeState) {
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

    private boolean isSystemProperty(String key) {
        // Add any system property filters here
        return false;
    }

    private void processVertexChange(JanusGraphVertex vertex, ChangeState changeState, String operationType,
            TransactionId txId, Map<String, Map<String, Object>> propertyDiffs) {
        try {

            // 2. Convert message using the Strategy Pattern
            // We now delegate all operations (including UPDATE) to the converter.
            // SunbirdLegacyMessageConverter logic has been updated to handle UPDATEs with
            // full snapshots.
            Map<String, Object> event = converter.convert(vertex, changeState, operationType, txId);

            // 3. Filter if status attribute is missing
            if (!hasStatusAttribute(event)) {
                logger.debug("Dropping event for node {} as it lacks 'status' attribute.", vertex.id());
                return;
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

    private boolean hasStatusAttribute(Map<String, Object> event) {
        try {
            if (event.containsKey("transactionData")) {
                Map<String, Object> txData = (Map<String, Object>) event.get("transactionData");
                if (txData != null && txData.containsKey("properties")) {
                    Map<String, Object> props = (Map<String, Object>) txData.get("properties");
                    return props != null && props.containsKey("status");
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

}
