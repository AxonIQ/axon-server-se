package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.internal.NodeInfo;

import java.util.List;

/**
 * Author: marc
 */
public interface RaftConfigService {

    void addNodeToContext(String name, String node);

    void deleteContext(String name);

    void deleteNodeFromContext(String name, String node);

    void addContext(String context, List<String> nodes);

    void join(NodeInfo nodeInfo);

    void init(List<String> contexts);
}
