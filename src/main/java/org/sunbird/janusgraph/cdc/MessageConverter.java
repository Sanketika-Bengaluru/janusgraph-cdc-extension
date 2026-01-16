package org.sunbird.janusgraph.cdc;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.TransactionId;

import java.util.Map;

public interface MessageConverter {
    Map<String, Object> convert(JanusGraphVertex vertex, ChangeState changeState, String operationType,
            TransactionId txId);
}
