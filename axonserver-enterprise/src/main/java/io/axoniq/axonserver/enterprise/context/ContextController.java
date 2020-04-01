package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.persistence.EntityManager;

/**
 * @author Marc Gathier
 */
@Controller
public class ContextController {
    private final Logger logger = LoggerFactory.getLogger(ContextController.class);
    private final EntityManager entityManager;
    private final ClusterController clusterController;

    public ContextController(
            EntityManager entityManager,
            ClusterController clusterController) {
        this.entityManager = entityManager;
        this.clusterController = clusterController;
    }

    public Stream<Context> getContexts() {
        return entityManager.createQuery("select c from Context c", Context.class)
                            .getResultList()
                            .stream();
    }

    public Context getContext(String contextName){
        return entityManager.find(Context.class, contextName);
    }

    @Transactional
    public void updateContext(ContextConfiguration contextConfiguration) {
        Context context = entityManager.find(Context.class, contextConfiguration.getContext());
        if( ! contextConfiguration.getPending() && contextConfiguration.getNodesCount() == 0) {
            if (context != null) {
                entityManager.remove(context);
            }
            return;
        }


        if( context == null) {
            context = new Context(contextConfiguration.getContext());
            entityManager.persist(context);
        }
        context.changePending(contextConfiguration.getPending());
        Map<String, ClusterNode> currentNodes = new HashMap<>();
        context.getAllNodes().forEach(n -> currentNodes.put(n.getClusterNode().getName(), n.getClusterNode()) );
        Map<String, NodeInfoWithLabel> newNodes = new HashMap<>();
        contextConfiguration.getNodesList().forEach(n -> newNodes.put(n.getNode().getNodeName(), n));

        Map<String, ClusterNode> clusterInfoMap = new HashMap<>();
        for (NodeInfoWithLabel nodeInfo : contextConfiguration.getNodesList()) {
            String nodeName = nodeInfo.getNode().getNodeName();
            ClusterNode clusterNode = getNode(nodeName);
            if( clusterNode == null) {
                logger.debug("{}: Creating new connection to {}", contextConfiguration.getContext(), nodeInfo.getNode().getNodeName());
                clusterNode = clusterController.addConnection(nodeInfo.getNode());
            }
            clusterInfoMap.put(nodeName, clusterNode);
        }

        Context finalContext = context;
        currentNodes.forEach((node, clusterNode) -> {
            if( !newNodes.containsKey(node)) {
                logger.debug("{}: Node not in new configuration {}", contextConfiguration.getContext(), node);
                clusterNode.removeContext(finalContext.getName());
            }
            });
        newNodes.forEach((node, nodeInfo) -> {
            if( !currentNodes.containsKey(node)) {
                logger.debug("{}: Node not in current configuration {}", contextConfiguration.getContext(), node);
                clusterInfoMap.get(node).addContext(finalContext, nodeInfo.getLabel());
            }
        });
    }

    public Iterable<String> getRemoteNodes() {
        return clusterController.remoteNodeNames();
    }

    public ClusterNode getNode(String node) {
        return entityManager.find(ClusterNode.class, node);
    }


    public void deleteContext(String context) {
        Context contextJpa = entityManager.find(Context.class, context);
        if( contextJpa != null) entityManager.remove(contextJpa);
    }

    @EventListener
    @Transactional
    public void on(ContextEvents.AdminContextDeleted contextDeleted) {
        List<Context> allContexts = entityManager.createQuery("select c from Context c").getResultList();
        allContexts.forEach(c -> entityManager.remove(c));
    }

    @Transactional
    public void addNode(NodeInfo nodeInfo) {
        clusterController.addConnection(nodeInfo);
    }

    public Collection<String> getMyContextNames() {
        return clusterController.getMe().getContextNames();
    }

    @Transactional
    public void deleteAll() {
        List<Context> allContexts = entityManager.createQuery("select c from Context c").getResultList();
        allContexts.forEach(context -> entityManager.remove(context));
    }
}
