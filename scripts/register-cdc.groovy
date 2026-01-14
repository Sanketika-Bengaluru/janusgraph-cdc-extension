// register-cdc.groovy
// This script is intended to be run by JanusGraph Server on startup or via gremlin-console

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.janusgraph.core.JanusGraphFactory

Logger logger = LoggerFactory.getLogger("register-cdc-script");

try {
    logger.info("Attempting to load GraphLogProcessor...");
    
    // Dynamically load the class to ensure it's on classpath
    Class<?> processorClass = Class.forName("org.sunbird.janusgraph.cdc.GraphLogProcessor");
    
    // Debug bindings
    try {
        logger.info("Available bindings: " + binding.getVariables().keySet());
    } catch (Exception e) {}

    def graphInstance = null;
    try {
        graphInstance = graph;
    } catch (MissingPropertyException e) {
        logger.error("Variable 'graph' not found in binding. CDC Processor NOT started.");
        return;
    }

    if (graphInstance != null) {
        logger.info("Found graph instance: " + graphInstance);
        
        // Prepare Configuration
        // We will try to extract properties from the graph configuration if possible,
        // or allow manual overrides here.
        Map<String, Object> config = new HashMap<>();
        
        // 1. Try to read from graph configuration (if it's a StandardJanusGraph)
        try {
             if (graphInstance.hasProperty("configuration")) {
                 // specific logic depending on how configuration is exposed
                 // simpler approach: just use the map below or allow user to set binding vars
             }
        } catch (Exception ignore) {}
        
        // 2. Set Default / User Configuration
        // USER: You can configure the CDC processor here
        config.put("graph.txn.log_processor.enable", "true");
        config.put("graph.txn.log_processor.sinks", "KAFKA,LOG");
        config.put("graph.txn.log_processor.converter", "TELEMETRY");
        
        // Kafka Configs (Optional if using defaults)
        config.put("kafka.bootstrap.servers", "kafka:29092");
        config.put("kafka.topics.graph.event", "sunbirddev.learning.graph.events");

        logger.info("CDC Config: " + config);
        
        // Call start method: start(JanusGraph graph, Map<String, Object> config)
        java.lang.reflect.Method startMethod = processorClass.getMethod("start", org.janusgraph.core.JanusGraph.class, Map.class);
        startMethod.invoke(null, graphInstance, config);
        
        logger.info("GraphLogProcessor invoked successfully.");
    } else {
        logger.error("Graph instance 'graph' not found in binding. CDC Processor NOT started.");
    }
} catch (ClassNotFoundException e) {
    logger.error("GraphLogProcessor class not found. Ensure janusgraph-cdc-extension jar is in /lib.", e);
} catch (NoSuchMethodException e) {
    logger.error("Start method not found. Check method signature.", e);
} catch (Exception e) {
    logger.error("Error starting GraphLogProcessor", e);
}
