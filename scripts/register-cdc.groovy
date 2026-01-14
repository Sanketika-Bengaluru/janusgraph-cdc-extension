// register-cdc.groovy
// This script is intended to be run by JanusGraph Server on startup or via gremlin-console

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.janusgraph.core.JanusGraphFactory

Logger logger = LoggerFactory.getLogger("register-cdc-script");

try {
    logger.info("Attempting to load GraphLogProcessor...");
    
    // In JanusGraph Server context, 'graph' variable is usually available if defined in gremlin-server.yaml
    // If not, we might need to get it from the globals or factory.
    // Assuming 'graph' is the graph instance name.
    
    // Dynamically load the class to ensure it's on classpath
    Class<?> processorClass = Class.forName("org.sunbird.janusgraph.cdc.GraphLogProcessor");
    
    // Call start method via reflection or direct call (Groovy allows dynamic dispatch, but static types help)
    // We pass the graph instance. 
    // IMPORTANT: Verify the variable name of your graph in gremlin-server.yaml "graphs" section.
    // Usually it is 'graph'.
    
    if (binding.hasVariable('graph')) {
        def graphInstance = graph;
        logger.info("Found graph instance: " + graphInstance);
        
        java.lang.reflect.Method startMethod = processorClass.getMethod("start", org.janusgraph.core.JanusGraph.class);
        startMethod.invoke(null, graphInstance);
        logger.info("GraphLogProcessor invoked successfully.");
    } else {
        logger.error("Graph instance 'graph' not found in binding. CDC Processor NOT started.");
    }
} catch (ClassNotFoundException e) {
    logger.error("GraphLogProcessor class not found. Ensure janusgraph-cdc-extension jar is in /lib.", e);
} catch (Exception e) {
    logger.error("Error starting GraphLogProcessor", e);
}
