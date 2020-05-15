package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.context.ContextRepository;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Component used for determining to which Axon Server node a client needs to connect. If the current node is an Admin node,
 * the candidates are retrieved from the global configuration, otherwise the candidates are retrieved from the RAFT configuration
 * of the context.
 * Clients can only connect to nodes with PRIMARY or MESSAGING_ONLY role.
 *
 * Maintains a set of connected Axon Server nodes to ensure that the candidate returned is actually active.
 *
 * Delegates the selection of the most suitable candidate to the {@link NodeSelectionStrategy}.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class NodeSelector {

    private final String nodeName;
    private final Function<String, ClusterNode> clusterNodeSelector;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final Function<String, Context> contextSelector;
    private final Function<String, Set<JpaRaftGroupNode>> raftNodeSelector;
    private final Iterable<String> activeConnections;

    /**
     * Constructor for testing purposes.
     *
     * @param nodeName              the name of the current node
     * @param nodeSelectionStrategy the node selection strategy to use
     * @param clusterNodeSelector   function to find a {@link ClusterNode} based on its name
     * @param contextSelector       function to fina a {@link Context} based on its name
     * @param raftNodeSelector      function to retrieve the {@link JpaRaftGroupNode}s for a context
     * @param activeConnections     provides names of axon server nodes that are currently connected to this node
     */
    NodeSelector(String nodeName,
                 NodeSelectionStrategy nodeSelectionStrategy,
                 Function<String, ClusterNode> clusterNodeSelector,
                 Function<String, Context> contextSelector,
                 Function<String, Set<JpaRaftGroupNode>> raftNodeSelector,
                 Iterable<String> activeConnections) {
        this.nodeName = nodeName;
        this.clusterNodeSelector = clusterNodeSelector;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.contextSelector = contextSelector;
        this.raftNodeSelector = raftNodeSelector;
        this.activeConnections = activeConnections;
    }

    /**
     * Autowired constructor.
     * @param messagingPlatformConfiguration the AxonServer configuration
     * @param nodeSelectionStrategy the {@link NodeSelectionStrategy} to use
     * @param clusterNodeRepository repository of nodes in the cluster
     * @param contextRepository repository of contexts defined in the cluster
     * @param raftGroupRepositoryManager    provides access to the raft group information
     * @param activeConnections provides names of axon server nodes that are currently connected to this node
     */
    @Autowired
    public NodeSelector(MessagingPlatformConfiguration messagingPlatformConfiguration,
                        NodeSelectionStrategy nodeSelectionStrategy,
                        ClusterNodeRepository clusterNodeRepository,
                        ContextRepository contextRepository,
                        RaftGroupRepositoryManager raftGroupRepositoryManager,
                        ActiveConnections activeConnections) {
        this(messagingPlatformConfiguration.getName(),
             nodeSelectionStrategy,
             n -> clusterNodeRepository.findById(n).orElse(null),
             c -> contextRepository.findById(c).orElse(null),
             raftGroupRepositoryManager::findByGroupId,
             activeConnections
        );
    }


    /**
     * Find the best node for a client to connect to.
     * @param clientName    the name of the client
     * @param componentName the component (application) name
     * @param context       the context where the client wants to connect to
     * @return the node where the client should connect to
     */
    public ClusterNode findNodeForClient(String clientName, String componentName, String context) {
        List<String> activeNodes = activeNodes(context);
        if (activeNodes.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.NO_AXONSERVER_FOR_CONTEXT,
                                                 "No active AxonServers found for context \"" + context + "\"");
        }
        ClusterNode me = clusterNodeSelector.apply(nodeName);
        if (clientName == null || clientName.isEmpty()) {
            return me;
        }

        String selectedNode = nodeSelectionStrategy.selectNode(new ClientIdentification(context, clientName),
                                                               componentName,
                                                               activeNodes);
        ClusterNode node = clusterNodeSelector.apply(selectedNode);
        if (node != null && !StringUtils.isEmpty(node.getHostName())) {
            return node;
        }
        return me;
    }

    /**
     * Checks if the given client is a candidate for re-balancing.
     * @param clientName    the name of the client
     * @param componentName the component (application) name
     * @param context       the context where the client wants to connect to
     * @return true if the client should move to another node
     */
    public boolean canRebalance(String clientName, String componentName, String context) {
        List<String> activeNodes = activeNodes(context);
        if (activeNodes.size() <= 1) {
            return false;
        }

        return nodeSelectionStrategy.canRebalance(new ClientIdentification(context, clientName),
                                                  componentName,
                                                  activeNodes);
    }

    private Collection<String> getNodesInContext(ClusterNode me, String context) {
        if (me.isAdmin()) {
            Context contextJPA = contextSelector.apply(context);
            if (contextJPA != null) {
                return contextJPA.getNodeNames(n -> RoleUtils.allowsClientConnect(n.getRole()) && !n.isPendingDelete());
            }
        }
        Set<JpaRaftGroupNode> nodes = raftNodeSelector.apply(context);
        return nodes.stream()
                    .filter(n -> !n.isPendingDelete() && RoleUtils.allowsClientConnect(n.getRole()))
                    .map(JpaRaftGroupNode::getNodeName)
                    .collect(Collectors.toSet());
    }


    private List<String> activeNodes(String context) {
        Collection<String> nodesInContext = getNodesInContext(clusterNodeSelector.apply(nodeName), context);
        return nodesInContext.stream()
                             .filter(n -> n.equals(nodeName) || contains(activeConnections, n))
                             .collect(Collectors.toList());
    }

    private boolean contains(Iterable<String> strings, String s) {
        for (String string : strings) {
            if (string.equals(s)) {
                return true;
            }
        }
        return false;
    }
}
