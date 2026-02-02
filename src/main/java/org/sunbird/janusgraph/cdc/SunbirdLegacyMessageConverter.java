package org.sunbird.janusgraph.cdc;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SunbirdLegacyMessageConverter implements MessageConverter {

    private static final Logger logger = LoggerFactory.getLogger(SunbirdLegacyMessageConverter.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
            .withZone(ZoneId.systemDefault());

    // Fields that should remain as JSON strings (loaded from config)
    private static final Set<String> STRING_ONLY_FIELDS = new HashSet<>();

    static {
        // Load configuration from cdc-converter.conf
        try (InputStream input = SunbirdLegacyMessageConverter.class.getClassLoader()
                .getResourceAsStream("cdc-converter.conf")) {
            if (input != null) {
                Properties prop = new Properties();
                prop.load(input);
                String fields = prop.getProperty("string.only.fields", "");
                if (!fields.isEmpty()) {
                    String[] fieldArray = fields.split(",");
                    for (String field : fieldArray) {
                        STRING_ONLY_FIELDS.add(field.trim());
                    }
                    logger.info("Loaded string-only fields: {}", STRING_ONLY_FIELDS);
                }
            } else {
                logger.warn("cdc-converter.conf not found, no string-only fields configured");
            }
        } catch (Exception e) {
            logger.error("Error loading cdc-converter.conf", e);
        }
    }

    @Override
    public Map<String, Object> convert(JanusGraphVertex vertex, ChangeState changeState, String operationType,
            TransactionId txId) {
        Map<String, Object> map = new HashMap<>();
        long ets = System.currentTimeMillis();
        String mid = UUID.randomUUID().toString();

        // Basic fields
        map.put("ets", ets);
        map.put("mid", mid);
        map.put("requestId", null); // Not available in CDC
        map.put("nodeGraphId", Long.parseLong(vertex.id().toString())); // Using vertex ID as numeric graph ID
        map.put("graphId", "domain"); // Default

        // Operation Type
        map.put("operationType", operationType);

        // Transaction Data
        Map<String, Object> transactionData = new HashMap<>();
        Map<String, Object> propertiesMap = new HashMap<>();

        // Populate Properties (nv/ov)
        // For CREATE: ov is null, nv is value
        // For DELETE: ov is confirmed value, nv is null
        // For UPDATE: logic required to find diff

        if ("CREATE".equals(operationType)) {
            vertex.properties().forEachRemaining(p -> {
                Map<String, Object> valMap = new HashMap<>();
                valMap.put("ov", null);
                valMap.put("nv", processValue(p.key(), p.value()));
                propertiesMap.put(p.key(), valMap);
            });
        } else if ("UPDATE".equals(operationType)) {
            Set<String> processedKeys = new HashSet<>();

            // 1. Process ADDED properties (new or updated values)
            try {
                changeState.getProperties(vertex, Change.ADDED).iterator().forEachRemaining(p -> {
                    String key = p.key();
                    processedKeys.add(key);
                    Map<String, Object> valMap = new HashMap<>();
                    valMap.put("nv", processValue(key, p.value()));

                    Object ov = null;
                    try {
                        // Fetch the removed property with the same key which represents the old value
                        Iterator<JanusGraphVertexProperty> removedProps = changeState
                                .getProperties(vertex, Change.REMOVED, key).iterator();
                        if (removedProps.hasNext()) {
                            ov = processValue(key, removedProps.next().value());
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                    valMap.put("ov", ov);
                    propertiesMap.put(key, valMap);
                });
            } catch (Exception e) {
                logger.warn("Error processing added properties for vertex {}", vertex.id(), e);
            }

            // 2. Process REMOVED properties (deleted properties not in ADDED)
            try {
                changeState.getProperties(vertex, Change.REMOVED).iterator().forEachRemaining(p -> {
                    String key = p.key();
                    if (!processedKeys.contains(key)) {
                        Map<String, Object> valMap = new HashMap<>();
                        valMap.put("ov", processValue(key, p.value()));
                        valMap.put("nv", null);
                        propertiesMap.put(key, valMap);
                    }
                });
            } catch (Exception e) {
                logger.warn("Error processing removed properties for vertex {}", vertex.id(), e);
            }

            // 3. Add ALL current vertex properties (complete snapshot)
            // This ensures every UPDATE event has the complete current state
            vertex.properties().forEachRemaining(p -> {
                String key = p.key();
                if (!processedKeys.contains(key)) {
                    Map<String, Object> valMap = new HashMap<>();
                    valMap.put("nv", processValue(key, p.value()));
                    valMap.put("ov", null); // Unchanged property
                    propertiesMap.put(key, valMap);
                }
            });
        } else if ("DELETE".equals(operationType)) {
            // In DELETE, changeState might provide REMOVED properties, or we access what we
            // can
            // Note: Vertex might be empty if already removed, but ChangeState should have
            // it.
            // However, for DELETE, we rely on what's available.
            // The passed 'vertex' is from getVertices(Change.REMOVED).
            // We can check removed properties if needed, or assume current state is 'ov'.
            // JanusGraph might not provide properties on a removed vertex handle easily.
            // We'll attempt to iterate properties if they exist in memory trace.

            // Strategy: Iterate properties if available. if not, we can't emit much.
            try {
                vertex.properties().forEachRemaining(p -> {
                    Map<String, Object> valMap = new HashMap<>();
                    valMap.put("ov", processValue(p.key(), p.value()));
                    valMap.put("nv", null);
                    propertiesMap.put(p.key(), valMap);
                });
            } catch (Exception e) {
                logger.warn("Could not read properties for deleted vertex {}", vertex.id());
            }
        }

        // Add properties to transactionData
        transactionData.put("properties", propertiesMap);

        // Placeholder for Relations/Tags as per legacy format (empty for now)
        transactionData.put("addedTags", new ArrayList<>());
        transactionData.put("removedTags", new ArrayList<>());
        transactionData.put("addedRelations", new ArrayList<>());
        transactionData.put("removedRelations", new ArrayList<>());

        map.put("transactionData", transactionData);

        // Derived Fields from props
        Map<String, Object> flatProps = flattenProperties(propertiesMap);

        map.put("createdOn", DATE_FORMATTER.format(Instant.now())); // Current time as we process
        map.put("channel", flatProps.getOrDefault("channel", "all"));

        // Label logic
        map.put("label", getLabel(vertex, flatProps));

        // Node Type
        map.put("nodeType", flatProps.getOrDefault("IL_SYS_NODE_TYPE", "DATA_NODE"));

        // Object Type
        map.put("objectType", flatProps.getOrDefault("IL_FUNC_OBJECT_TYPE", vertex.label()));

        // Unique ID
        map.put("nodeUniqueId", flatProps.getOrDefault("IL_UNIQUE_ID", vertex.id().toString()));

        // User ID
        map.put("userId", getUserId(flatProps));

        return map;
    }

    private Map<String, Object> flattenProperties(Map<String, Object> propertiesMap) {
        Map<String, Object> flat = new HashMap<>();
        for (Map.Entry<String, Object> entry : propertiesMap.entrySet()) {
            Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
            // multiple logic: prefer nv, else ov
            Object val = valMap.get("nv");
            if (val == null)
                val = valMap.get("ov");
            if (val != null)
                flat.put(entry.getKey(), val);
        }
        return flat;
    }

    private String getLabel(JanusGraphVertex vertex, Map<String, Object> props) {
        // Legacy: name -> lemma -> title -> gloss
        if (props.containsKey("name"))
            return (String) props.get("name");
        if (props.containsKey("lemma"))
            return (String) props.get("lemma");
        if (props.containsKey("title"))
            return (String) props.get("title");
        if (props.containsKey("gloss"))
            return (String) props.get("gloss");
        return vertex.label();
    }

    private String getUserId(Map<String, Object> props) {
        if (props.containsKey("lastUpdatedBy"))
            return (String) props.get("lastUpdatedBy");
        if (props.containsKey("createdBy"))
            return (String) props.get("createdBy");
        return "ANONYMOUS";
    }

    /**
     * Process property value based on configuration.
     * Parse JSON strings into objects/arrays for ALL fields EXCEPT those in
     * STRING_ONLY_FIELDS.
     */
    private Object processValue(String key, Object value) {
        // If field is in STRING_ONLY_FIELDS, keep it as string (don't parse)
        if (STRING_ONLY_FIELDS.contains(key)) {
            return value;
        }

        // For all other fields, try to parse JSON strings
        if (value instanceof String) {
            String str = (String) value;
            try {
                if ((str.startsWith("{") && str.endsWith("}")) || (str.startsWith("[") && str.endsWith("]"))) {
                    return mapper.readValue(str, Object.class);
                }
            } catch (Exception e) {
                // Return original string if parsing fails
            }
        }
        return value;
    }
}
