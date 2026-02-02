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
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

    // LRU Cache for timestamp-based deduplication (nodeUniqueId ->
    // lastProcessedTimestamp)
    private Map<String, Instant> lastProcessedTimestamps = new LinkedHashMap<String, Instant>(1000, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Instant> eldest) {
            return size() > 1000;
        }
    };

    // Event buffering for merging
    private Map<String, List<BufferedEvent>> eventBuffer = new ConcurrentHashMap<>();
    private long bufferWindowMs = 1000; // 1 second default
    private ScheduledExecutorService scheduler;

    /**
     * Buffered event with timestamp information
     */
    private static class BufferedEvent {
        Map<String, Object> event;
        Instant lastUpdatedOn;
        Instant receivedAt;

        BufferedEvent(Map<String, Object> event, Instant lastUpdatedOn) {
            this.event = event;
            this.lastUpdatedOn = lastUpdatedOn;
            this.receivedAt = Instant.now();
        }
    }

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

            // Configure buffer window
            if (config.containsKey("eventBufferWindowMs")) {
                bufferWindowMs = ((Number) config.get("eventBufferWindowMs")).longValue();
            }
            logger.info("Event buffer window set to {}ms", bufferWindowMs);

            // Start buffer flusher
            startBufferFlusher();

            isStarted = true;
            logger.info("GraphLogProcessor started successfully with {} sinks.", sinks.size());

        } catch (Exception e) {
            logger.error("Failed to start GraphLogProcessor", e);
        }
    }

    private void stop() {
        // Stop scheduler and flush remaining events
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Flush any remaining buffered events
        flushAllBufferedEvents();

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

            // 3. Filter based on lastUpdatedOn timestamp (skip older events)
            String nodeUniqueId = (String) event.get("nodeUniqueId");
            Instant currentTimestamp = getLastUpdatedOn(event);

            if (nodeUniqueId != null && currentTimestamp != null) {
                Instant lastProcessed = lastProcessedTimestamps.get(nodeUniqueId);
                if (lastProcessed != null && !currentTimestamp.isAfter(lastProcessed)) {
                    logger.info("Skipping older/duplicate event for node {} (current: {}, last processed: {})",
                            nodeUniqueId, currentTimestamp, lastProcessed);
                    return;
                }
                logger.info("Processing event for node {} with timestamp {} (last processed: {})",
                        nodeUniqueId, currentTimestamp, lastProcessed);
            } else {
                logger.warn("Cannot perform timestamp filtering for node {} (nodeUniqueId: {}, timestamp: {})",
                        vertex.id(), nodeUniqueId, currentTimestamp);
            }

            // 4. Filter if status attribute is missing
            if (!hasStatusAttribute(event)) {
                logger.debug("Dropping event for node {} as it lacks 'status' attribute.", vertex.id());
                return;
            }

            // 5. Buffer event for merging (instead of sending immediately)
            if (nodeUniqueId != null && currentTimestamp != null) {
                BufferedEvent bufferedEvent = new BufferedEvent(event, currentTimestamp);
                eventBuffer.computeIfAbsent(nodeUniqueId, k -> new CopyOnWriteArrayList<>()).add(bufferedEvent);
                logger.debug("Buffered event for node {} with timestamp {}", nodeUniqueId, currentTimestamp);
            } else {
                // If we can't extract timestamp, send immediately (fallback)
                logger.warn("Cannot buffer event for node {}, sending immediately", vertex.id());
                sendEventToSinks(vertex.id().toString(), event);
            }

        } catch (Exception e) {
            logger.error("Error converting/processing vertex change event", e);
        }
    }

    /**
     * Send event to all configured sinks
     */
    private void sendEventToSinks(String key, Map<String, Object> event) {
        try {
            String json = mapper.writeValueAsString(event);
            for (EventSink sink : sinks) {
                try {
                    sink.send(key, json);
                } catch (Exception e) {
                    logger.error("Error sending event to sink: {}", sink.getClass().getSimpleName(), e);
                }
            }
            logger.debug("Sent event: {}", json);
        } catch (Exception e) {
            logger.error("Error serializing event", e);
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

    /**
     * Extract lastUpdatedOn timestamp from event for deduplication.
     */
    private Instant getLastUpdatedOn(Map<String, Object> event) {
        try {
            if (event.containsKey("transactionData")) {
                Map<String, Object> txData = (Map<String, Object>) event.get("transactionData");
                if (txData != null && txData.containsKey("properties")) {
                    Map<String, Object> props = (Map<String, Object>) txData.get("properties");
                    if (props != null && props.containsKey("lastUpdatedOn")) {
                        Map<String, Object> lastUpdatedOnMap = (Map<String, Object>) props.get("lastUpdatedOn");
                        if (lastUpdatedOnMap != null) {
                            String timestamp = (String) lastUpdatedOnMap.get("nv");
                            if (timestamp != null) {
                                return Instant.parse(timestamp.replace("+0000", "Z"));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Could not extract lastUpdatedOn timestamp", e);
        }
        return null;
    }

    /**
     * Start scheduled thread to flush expired buffered events
     */
    private void startBufferFlusher() {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                flushExpiredEvents();
            } catch (Exception e) {
                logger.error("Error flushing buffered events", e);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
        logger.info("Buffer flusher started (check interval: 100ms)");
    }

    /**
     * Flush events that have been buffered longer than the window
     */
    private void flushExpiredEvents() {
        Instant now = Instant.now();
        List<String> keysToFlush = new ArrayList<>();

        for (Map.Entry<String, List<BufferedEvent>> entry : eventBuffer.entrySet()) {
            String nodeUniqueId = entry.getKey();
            List<BufferedEvent> events = entry.getValue();

            if (!events.isEmpty()) {
                BufferedEvent oldest = events.get(0);
                long ageMs = now.toEpochMilli() - oldest.receivedAt.toEpochMilli();

                if (ageMs >= bufferWindowMs) {
                    keysToFlush.add(nodeUniqueId);
                }
            }
        }

        for (String key : keysToFlush) {
            flushBufferedEvents(key);
        }
    }

    /**
     * Flush all buffered events (called during shutdown)
     */
    private void flushAllBufferedEvents() {
        logger.info("Flushing all buffered events...");
        for (String nodeUniqueId : new ArrayList<>(eventBuffer.keySet())) {
            flushBufferedEvents(nodeUniqueId);
        }
    }

    /**
     * Merge and send buffered events for a specific node
     */
    private void flushBufferedEvents(String nodeUniqueId) {
        List<BufferedEvent> events = eventBuffer.remove(nodeUniqueId);
        if (events == null || events.isEmpty()) {
            return;
        }

        if (events.size() == 1) {
            // Single event, send as-is
            BufferedEvent buffered = events.get(0);
            sendEventToSinks(nodeUniqueId, buffered.event);
            lastProcessedTimestamps.put(nodeUniqueId, buffered.lastUpdatedOn);
        } else {
            // Multiple events, merge them
            logger.info("Merging {} events for node {}", events.size(), nodeUniqueId);
            Map<String, Object> mergedEvent = mergeEvents(events);

            // Use the latest timestamp
            Instant latestTimestamp = events.stream()
                    .map(e -> e.lastUpdatedOn)
                    .max(Instant::compareTo)
                    .orElse(Instant.now());

            sendEventToSinks(nodeUniqueId, mergedEvent);
            lastProcessedTimestamps.put(nodeUniqueId, latestTimestamp);
        }
    }

    /**
     * Merge multiple events into a single event with the latest values
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeEvents(List<BufferedEvent> events) {
        // Sort by lastUpdatedOn (oldest to newest)
        events.sort(Comparator.comparing(e -> e.lastUpdatedOn));

        // Start with the first event as base
        Map<String, Object> merged = new HashMap<>(events.get(0).event);

        // Merge properties from subsequent events
        for (int i = 1; i < events.size(); i++) {
            Map<String, Object> event = events.get(i).event;

            // Merge transactionData.properties
            if (event.containsKey("transactionData") && merged.containsKey("transactionData")) {
                Map<String, Object> eventTxData = (Map<String, Object>) event.get("transactionData");
                Map<String, Object> mergedTxData = (Map<String, Object>) merged.get("transactionData");

                if (eventTxData.containsKey("properties") && mergedTxData.containsKey("properties")) {
                    Map<String, Object> eventProps = (Map<String, Object>) eventTxData.get("properties");
                    Map<String, Object> mergedProps = (Map<String, Object>) mergedTxData.get("properties");

                    // For each property in the newer event
                    for (Map.Entry<String, Object> propEntry : eventProps.entrySet()) {
                        String propKey = propEntry.getKey();
                        Map<String, Object> newPropValue = (Map<String, Object>) propEntry.getValue();

                        // Check if this property actually changed by comparing with previous event
                        boolean shouldUpdate = false;

                        if (!mergedProps.containsKey(propKey)) {
                            // New property, add it
                            shouldUpdate = true;
                        } else {
                            // Property exists in previous event, check if value changed
                            Map<String, Object> oldPropValue = (Map<String, Object>) mergedProps.get(propKey);
                            Object oldNv = oldPropValue.get("nv");
                            Object newNv = newPropValue.get("nv");

                            // Update if the nv value is different
                            if (!Objects.equals(oldNv, newNv)) {
                                shouldUpdate = true;
                            }
                        }

                        if (shouldUpdate) {
                            mergedProps.put(propKey, newPropValue);
                        }
                        // Otherwise, keep the existing value from the previous event
                    }
                }
            }

            // Update top-level fields from latest event
            merged.put("ets", event.get("ets"));
            merged.put("mid", event.get("mid"));
            merged.put("label", event.get("label"));
            merged.put("createdOn", event.get("createdOn"));
        }

        return merged;
    }
}
