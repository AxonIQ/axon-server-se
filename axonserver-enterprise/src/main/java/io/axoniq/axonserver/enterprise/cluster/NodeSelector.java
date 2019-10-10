package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;

/**
 * @author Marc Gathier
 */
@Component
public class NodeSelector {

    private final Set<String> activeConnections = new CopyOnWriteArraySet<>();
    private final String nodeName;
    private final Function<String, ClusterNode> clusterNodeSelector;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final Function<String, Context> contextSelector;
    private final Function<String, Set<JpaRaftGroupNode>> raftNodeSelector;

    public NodeSelector(String nodeName,
                        NodeSelectionStrategy nodeSelectionStrategy,
                        Function<String, ClusterNode> clusterNodeSelector,
                        Function<String, Context> contextSelector,
                        Function<String, Set<JpaRaftGroupNode>> raftNodeSelector) {
        this.nodeName = nodeName;
        this.clusterNodeSelector = clusterNodeSelector;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.contextSelector = contextSelector;
        this.raftNodeSelector = raftNodeSelector;
    }

    @Autowired
    public NodeSelector(MessagingPlatformConfiguration messagingPlatformConfiguration,
                        NodeSelectionStrategy nodeSelectionStrategy,
                        EntityManager entityManager,
                        RaftGroupRepositoryManager raftGroupRepositoryManager) {
        this(messagingPlatformConfiguration.getName(),
             nodeSelectionStrategy,
             node -> entityManager.find(ClusterNode.class, node),
             context -> entityManager.find(Context.class, context),
             raftGroupRepositoryManager::findByGroupId
        );
    }


    public ClusterNode findNodeForClient(String clientName, String componentName, String context) {
        List<String> activeNodes = activeNodes(context);
        if (activeNodes.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.NO_AXONSERVER_FOR_CONTEXT,
                                                 "No active AxonServers found for context: " + context);
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

    public boolean canRebalance(String clientName, String componentName, String context) {
        List<String> activeNodes = activeNodes(context);
        if (activeNodes.size() <= 1) {
            return false;
        }

        return nodeSelectionStrategy.canRebalance(new ClientIdentification(context, clientName),
                                                  componentName,
                                                  activeNodes);
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected connected) {
        activeConnections.add(connected.getNodeName());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected connected) {
        activeConnections.add(connected.getNodeName());
    }

    private Collection<String> getNodesInContext(ClusterNode me, String context) {
        if (me.isAdmin()) {
            Context contextJPA = contextSelector.apply(context);
            if (contextJPA != null) {
                return contextJPA.getNodeNames(n -> RoleUtils.allowsClientConnect(n.getRole()));
            }
        }
        Set<JpaRaftGroupNode> nodes = raftNodeSelector.apply(context);
        return nodes.stream()
                    .filter(n -> RoleUtils.allowsClientConnect(n.getRole()))
                    .map(JpaRaftGroupNode::getNodeName)
                    .collect(Collectors.toSet());
    }


    private List<String> activeNodes(String context) {
        Collection<String> nodesInContext = getNodesInContext(clusterNodeSelector.apply(nodeName), context);
        return nodesInContext.stream()
                             .filter(n -> n.equals(nodeName) || activeConnections.contains(n))
                             .collect(Collectors.toList());
    }
}
