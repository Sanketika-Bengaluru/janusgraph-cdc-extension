package org.sunbird.janusgraph.cdc;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SunbirdLegacyMessageConverter implements MessageConverter {

    private static final Logger logger = LoggerFactory.getLogger(SunbirdLegacyMessageConverter.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
            .withZone(ZoneId.systemDefault());

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
                valMap.put("nv", p.value());
                propertiesMap.put(p.key(), valMap);
            });
        } else if ("UPDATE".equals(operationType)) {
            vertex.properties().forEachRemaining(p -> {
                Map<String, Object> valMap = new HashMap<>();
                Object ov = null;
                try {
                    // Fetch the removed property with the same key for this vertex
                    // This represents the old value
                    Iterator<JanusGraphVertexProperty> removedProps = changeState
                            .getProperties(vertex, Change.REMOVED, p.key()).iterator();
                    if (removedProps.hasNext()) {
                        ov = removedProps.next().value();
                    }
                } catch (Exception e) {
                    logger.debug("Failed to fetch old value for key {}", p.key());
                }
                valMap.put("ov", ov);
                valMap.put("nv", p.value());
                propertiesMap.put(p.key(), valMap);
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
                    valMap.put("ov", p.value());
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
}
