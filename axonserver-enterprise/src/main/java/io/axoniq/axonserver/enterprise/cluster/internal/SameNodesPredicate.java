package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiPredicate;

/**
 * Predicate that checks if two {@link ContextConfiguration contextConfiguration} contains the same nodes.
 */
public class SameNodesPredicate implements BiPredicate<ContextConfiguration, ContextConfiguration> {
    private static final Logger logger = LoggerFactory.getLogger(SameNodesPredicate.class);

    @Override
    public boolean test(ContextConfiguration configuration, ContextConfiguration configuration2) {
        if (configuration.getNodesCount() != configuration2.getNodesCount()) {
            return false;
        }
        for (NodeInfoWithLabel node : configuration.getNodesList()) {
            boolean found = false;
            for (NodeInfoWithLabel node2 : configuration2.getNodesList()) {
                if (node.getLabel().equals(node2.getLabel())) {

                    found = true;
                    NodeInfo n1 = node.getNode();
                    NodeInfo n2 = node2.getNode();
                    if (!n1.getInternalHostName().equals(n2.getInternalHostName()) ||
                            n1.getGrpcInternalPort() != n2.getGrpcInternalPort() ||
                            !n1.getNodeName().equals(n2.getNodeName())) {
                        logger.warn("{}: Node with different configuration, first: {} -> {}:{}, second {} -> {}:{}",
                                    node.getLabel(),
                                    n1.getNodeName(),
                                    n1.getInternalHostName(),
                                    n1.getGrpcInternalPort(),
                                    n2.getNodeName(),
                                    n2.getInternalHostName(),
                                    n2.getGrpcInternalPort()
                                    );
                        return false;
                    }
                }
            }
            if (!found) {
                logger.warn("{}: Node not found", node.getLabel());
                return false;
            }
        }
        return true;
    }

}
