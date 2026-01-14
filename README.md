# JanusGraph CDC Log Processor - Setup Guide

## Overview

The JanusGraph CDC (Change Data Capture) Log Processor is a **standalone JAR** that runs **inside JanusGraph Server**. It automatically captures graph mutations and publishes them to Kafka **without requiring any application code changes**.

### How It Works

```
Application → JanusGraph Server (with logIdentifier)
                    ↓
            Transaction Log (Cassandra)
                    ↓
            GraphLogProcessor JAR (polls log)
                    ↓
            Kafka Topic (events published)
```

**Key Point**: The JAR runs in a separate thread inside JanusGraph Server, not in your application. Your application only needs to tag transactions with a `logIdentifier`.

---

## Project Structure

- **`src/main/java`**: Contains the Java source code for the extension.
    - `GraphLogProcessor.java`: The core logic for processing transaction logs.
    - `KafkaEventProducer.java`: Handles publishing events to Kafka.
- **`scripts/`**: Contains Groovy scripts for JanusGraph.
    - `register-cdc.groovy`: Script to register the CDC processor with JanusGraph.
    - `empty-sample.groovy`: Sample bootstrap script.
- **`.github/workflows`**: CI/CD pipeline definitions.
- **`pom.xml`**: Maven build configuration.

---

## Prerequisites

- JanusGraph Server 1.1.0 (running in Docker)
- Kafka (accessible from JanusGraph container)
- Maven (for building the JAR)
- Application using JanusGraph

---

## Step 1: Build the CDC Extension JAR

### 1.1 Navigate to Project
```bash
cd janusgraph-cdc-extension
```

### 1.2 Build the JAR
```bash
mvn clean package -DskipTests
```

**Output**: `target/janusgraph-cdc-extension-1.0-SNAPSHOT.jar`

**What this JAR contains**:
- `GraphLogProcessor.java` - Main processor that reads transaction logs
- `KafkaEventProducer.java` - Kafka publisher
- Shaded Kafka client libraries (to avoid conflicts)

---

## Step 2: Deploy JAR to JanusGraph Server

### 2.1 Copy JAR to Container
```bash
docker cp janusgraph-cdc-extension/target/janusgraph-cdc-extension-1.0-SNAPSHOT.jar \
  sunbird_janusgraph:/opt/janusgraph/lib/
```

### 2.2 Verify JAR is Present
```bash
docker exec sunbird_janusgraph ls -lh /opt/janusgraph/lib/janusgraph-cdc-extension-1.0-SNAPSHOT.jar
```

---

## Step 3: Configure Transaction Log Backend

### 3.1 Add Log Configuration to Template
```bash
docker exec sunbird_janusgraph sh -c 'cat >> /opt/janusgraph/conf/janusgraph-cql-server.properties << EOF
# CDC Transaction Log Configuration
log.learning_graph_events.backend=default
log.learning_graph_events.key-consistent=true
log.learning_graph_events.read-interval=500
EOF'
```

**Explanation**:
- `backend=default` - Use the same Cassandra backend as the graph
- `key-consistent=true` - Ensure log entries are consistently ordered
- `read-interval=500` - Poll for new log entries every 500ms

### 3.2 Verify Configuration
```bash
docker exec sunbird_janusgraph cat /opt/janusgraph/conf/janusgraph-cql-server.properties | grep learning_graph_events
```

Expected output:
```
log.learning_graph_events.backend=default
log.learning_graph_events.key-consistent=true
log.learning_graph_events.read-interval=500
```

---

## Step 4: Configure Bootstrap Script

The bootstrap script (`scripts/register-cdc.groovy`) is configured to:
1. Load the `GraphLogProcessor` class
2. Start it with the server's graph instance
3. Pass Configuration (Sinks, Converters, Kafka)

**Location**: `/opt/janusgraph/scripts/register-cdc.groovy`

**Configuration**:
You can modify the configuration map in the script:

```groovy
Map<String, Object> config = new HashMap<>();
// Sinks: KAFKA, LOG
config.put("graph.txn.log_processor.sinks", "KAFKA,LOG"); 
// Converter: TELEMETRY (Custom), DEFAULT (Legacy)
config.put("graph.txn.log_processor.converter", "TELEMETRY"); 
config.put("kafka.bootstrap.servers", "localhost:9092");
config.put("kafka.topics.graph.event", "sunbirddev.learning.graph.events");

GraphLogProcessor.start(graphInstance, config);
```

---

## Step 5: Configure Logging (Optional)

If you enabled the **LOG** sink, events are written to the standard JanusGraph server log by default.
To write these events to a separate file, you need to add an Appender and Logger to your `/opt/janusgraph/conf/log4j2-server.xml`.

**Example Configuration (`src/main/resources/log4j2-server.xml`)**:

```xml
<Configuration>
    <Appenders>
        <!-- Existing Appenders... -->
        
        <!-- CDC Appender -->
        <RollingFile name="CDC_LOG" fileName="/opt/janusgraph/logs/cdc-events.log"
                     filePattern="/opt/janusgraph/logs/cdc-events-%d{yyyy-MM-dd}.log.gz">
            <PatternLayout pattern="%d{ISO8601} %msg%n"/>
            <Policies>
                <TimeBasedTriggeringPolicy />
                <SizeBasedTriggeringPolicy size="100 MB"/>
            </Policies>
        </RollingFile>
    </Appenders>
    <Loggers>
        <!-- Existing Loggers... -->
        
        <!-- CDC Logger -->
        <Logger name="org.sunbird.janusgraph.cdc.LogFileEventSink" level="info" additivity="false">
            <AppenderRef ref="CDC_LOG"/>
        </Logger>
    </Loggers>
</Configuration>
```

To apply this in Docker:
```bash
docker cp src/main/resources/log4j2-server.xml sunbird_janusgraph:/opt/janusgraph/conf/
docker exec sunbird_janusgraph mkdir -p /opt/janusgraph/logs
docker restart sunbird_janusgraph
```

You can then tail the logs:
```bash
docker exec sunbird_janusgraph tail -f /opt/janusgraph/logs/cdc-events.log
```

---

## Step 6: Restart JanusGraph Server

```bash
docker restart sunbird_janusgraph
```

Wait ~30 seconds for startup, then verify:

```bash
docker logs sunbird_janusgraph | grep "GraphLogProcessor started successfully"
```

Expected output:
```
INFO  org.sunbird.janusgraph.cdc.GraphLogProcessor - GraphLogProcessor started successfully with 2 sinks.
```

---

## Step 6: Configure Your Application

### 6.1 Enable Transaction Logging
Add to your application configuration (e.g., `application.conf`):

```hocon
graph.txn.enable_log = true
```

### 6.2 Verify Application Code
Your application should use this pattern:

```java
if (TXN_LOG_ENABLED) {
    JanusGraph graph = DriverUtil.getJanusGraph(graphId);
    JanusGraphTransaction tx = graph.buildTransaction()
        .logIdentifier("learning_graph_events")
        .start();
    GraphTraversalSource g = tx.traversal();
    
    // ... perform graph operations ...
    
    tx.commit();
}
```

**Important**: The `logIdentifier("learning_graph_events")` must match the log name in JanusGraph configuration.

---

## Step 7: Verify CDC Pipeline is Working

### 7.1 Check GraphLogProcessor Status
```bash
docker logs sunbird_janusgraph | grep -E "GraphLogProcessor|CDC:"
```

Expected output:
```
INFO  org.sunbird.janusgraph.cdc.GraphLogProcessor.start - Starting GraphLogProcessor...
INFO  org.sunbird.janusgraph.cdc.GraphLogProcessor.start - Kafka Bootstrap Servers: kafka:29092
INFO  org.sunbird.janusgraph.cdc.GraphLogProcessor.start - Kafka Topic: sunbirddev.learning.graph.events
INFO  org.sunbird.janusgraph.cdc.GraphLogProcessor.start - GraphLogProcessor started successfully.
```

### 7.2 Create Test Content via API
```bash
curl -X POST http://localhost:9000/content/v3/create \
  -H "Content-Type: application/json" \
  -H "X-Channel-Id: test" \
  -d '{
    "request": {
      "content": {
        "name": "CDC Test Content",
        "code": "test.cdc.001",
        "mimeType": "application/pdf",
        "primaryCategory": "Learning Resource",
        "createdBy": "test-user",
        "channel": "sunbird"
      }
    }
  }'
```

### 7.3 Check for CDC Event Logs
```bash
docker logs sunbird_janusgraph | grep "CDC: Received transaction"
```

Expected output:
```
INFO  org.sunbird.janusgraph.cdc.GraphLogProcessor - CDC: Received transaction id: ...
```

### 7.4 Verify Events in Kafka
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic sunbirddev.learning.graph.events \
  --from-beginning \
  --timeout-ms 3000
```

Expected output (JSON):
```json
{
  "nodeGraphId": "domain",
  "nodeUniqueId": "do_12345...",
  "operationType": "CREATE",
  "timestamp": 1768375015227,
  "txId": "...",
  "objectType": "Content",
  "properties": {
    "IL_UNIQUE_ID": "do_12345...",
    "name": "CDC Test Content",
    "code": "test.cdc.001",
    ...
  }
}
```

---

## Troubleshooting

### No Events in Kafka?

**1. Check GraphLogProcessor Started**:
```bash
docker logs sunbird_janusgraph | grep "GraphLogProcessor started successfully"
```

**2. Check Kafka Connection**:
```bash
docker logs sunbird_janusgraph | grep "Kafka"
```

**3. Verify Application Uses logIdentifier**:
```bash
docker logs content-service | grep "Initialized JanusGraph Transaction with Log Identifier"
```

**4. Check Transaction Log Configuration**:
```bash
docker exec sunbird_janusgraph cat /etc/opt/janusgraph/janusgraph.properties | grep learning_graph_events
```

### Events Missing Properties?

Edit `GraphLogProcessor.java` → `processVertexChange()` method to adjust property extraction logic.

### Performance Issues?

- Reduce `log.learning_graph_events.read-interval` for faster polling
- Monitor Kafka producer metrics in JanusGraph logs
- Consider batching for high-volume scenarios
