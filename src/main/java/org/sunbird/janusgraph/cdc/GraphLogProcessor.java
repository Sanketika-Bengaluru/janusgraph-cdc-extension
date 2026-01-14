package org.sunbird.janusgraph.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * GraphLogProcessor running inside JanusGraph Server.
 * Listens to "learning_graph_events" user log and publishes changes to Kafka.
 */
public class GraphLogProcessor {

    private static final Logger logger = LoggerFactory.getLogger(GraphLogProcessor.class);
    private static final String LOG_IDENTIFIER = "learning_graph_events";
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private static KafkaEventProducer kafkaProducer;
    private static boolean isStarted = false;

    public static synchronized void start(JanusGraph graph, Map<String, Object> config) {
        if (isStarted) {
            logger.warn("GraphLogProcessor is already running.");
            return;
        }

        // Configuration passed from script
        boolean enableProcessor = Boolean.parseBoolean(String.valueOf(config.getOrDefault("graph.txn.log_processor.enable", "false")));
        if (!enableProcessor) {
            logger.info("GraphLogProcessor is disabled by config.");
            return;
        }

        String kafkaServers = (String) config.getOrDefault("kafka.bootstrap.servers", "localhost:9092");
        String kafkaTopic = (String) config.getOrDefault("kafka.topics.graph.event", "sunbirddev.learning.graph.events");

        logger.info("Starting GraphLogProcessor...");
        logger.info("Kafka Bootstrap Servers: {}", kafkaServers);
        logger.info("Kafka Topic: {}", kafkaTopic);

        try {
            kafkaProducer = new KafkaEventProducer(kafkaServers, kafkaTopic);

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
            logger.info("GraphLogProcessor started successfully.");

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
            // Filter logic: Only process "DATA_NODE" or specific types if needed
            // For now, capture all.
            
            // Note: For REMOVED vertices, properties might not be accessible if they are wiped.
            // ChangeState should provide the state of the vertex.
            
            Map<String, Object> event = new HashMap<>();
            event.put("nodeGraphId", "domain"); // Hardcoded or derived
            event.put("nodeUniqueId", vertex.id().toString()); // Internal ID, or property 'IL_UNIQUE_ID'
            event.put("operationType", operationType);
            event.put("timestamp", System.currentTimeMillis());
            event.put("txId", txId.toString());

            // Extract Label
            event.put("objectType", vertex.label());

            // Extract Properties
            // For CREATE, we want full snapshot.
            if ("CREATE".equals(operationType)) {
               Map<String, Object> properties = new HashMap<>();
               vertex.properties().forEachRemaining(p -> {
                   properties.put(p.key(), p.value());
               });
               event.put("properties", properties);
               
               // Try to find a better unique ID if available
               if (properties.containsKey("IL_UNIQUE_ID")) {
                   event.put("nodeUniqueId", properties.get("IL_UNIQUE_ID"));
               }
            }

            String json = mapper.writeValueAsString(event);
            kafkaProducer.send(vertex.id().toString(), json);
            logger.debug("Published event: {}", json);

        } catch (Exception e) {
            logger.error("Error converting/sending vertex change event", e);
        }
    }

    public static synchronized void shutdown() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        isStarted = false;
        logger.info("GraphLogProcessor stopped.");
    }
}
