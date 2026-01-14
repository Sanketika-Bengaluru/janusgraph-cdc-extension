package org.sunbird.janusgraph.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                    .setStartTime(Instant.now().minus(1, ChronoUnit.DAYS)) // Replay last 24h on first start
                    .addProcessor(new ChangeProcessor() {
                        @Override
                        public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
                            logger.info("CDC: Received transaction id: {}", txId);
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
        // Iterate over specific changes we care about (Vertices)
        for (JanusGraphVertex vertex : changeState.getVertices(Change.ADDED)) {
            processVertexChange(vertex, "CREATE", txId);
        }

        for (JanusGraphVertex vertex : changeState.getVertices(Change.REMOVED)) {
            processVertexChange(vertex, "DELETE", txId);
        }
    }

    private static void processVertexChange(JanusGraphVertex vertex, String operationType, TransactionId txId) {
        try {
            // Convert message
            Map<String, Object> event = converter.convert(vertex, operationType, txId);
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
