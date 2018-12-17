package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.persistence.EntityManager;

/**
 * Author: marc
 */
@Controller
public class ContextController {
    private final EntityManager entityManager;
    private final ClusterController clusterController;

    public ContextController(
            EntityManager entityManager,
            ClusterController clusterController,
            ApplicationEventPublisher eventPublisher) {
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

        if( context == null) {
            context = new Context(contextConfiguration.getContext());
            entityManager.persist(context);
        }
        Set<String> currentNodes = context.getAllNodes().stream().map(n -> n.getClusterNode().getName()).collect(Collectors.toSet());
        Set<String> newNodes = contextConfiguration.getNodesList().stream().map(NodeInfo::getNodeName).collect(Collectors.toSet());

        Map<String, ClusterNode> clusterInfoMap = new HashMap<>();
        for (NodeInfo nodeInfo : contextConfiguration.getNodesList()) {
            ClusterNode clusterNode = clusterController.getNode(nodeInfo.getNodeName());
            if( clusterNode == null) {
                clusterNode = clusterController.addConnection(nodeInfo, false);
            }
            clusterInfoMap.put(nodeInfo.getNodeName(), clusterNode);
        }

        Context finalContext = context;
        currentNodes.forEach(node -> {
            if( !newNodes.contains(node)) {
                ClusterNode clusterNode = clusterController.getNode(node);
                if( clusterNode !=null) {
                    clusterNode.removeContext(finalContext.getName());
                }
            }
            });
        newNodes.forEach(node -> {
            if( !currentNodes.contains(node)) {
                clusterInfoMap.computeIfAbsent(node, this::getNode).addContext(finalContext, false, false);
            }
        });

    }

    public List<Node> getNodes(List<String> nodes) {
        return nodes.stream().map(clusterController::getNode)
                    .map(ClusterNode::toNode)
                    .collect(Collectors.toList());

    }

    public Iterable<String> getNodes() {
        return entityManager.createQuery("select n.name from ClusterNode n", String.class).getResultList();
    }
    public List<NodeInfo> getNodeInfos(List<String> nodes) {
        return nodes.stream().map(clusterController::getNode)
                    .map(ClusterNode::toNodeInfo)
                    .collect(Collectors.toList());
    }

    public ClusterNode getNode(String node) {
        return clusterController.getNode(node);
    }


    public void deleteContext(String context) {
        Context contextJpa = entityManager.find(Context.class, context);
        if( contextJpa != null) entityManager.remove(contextJpa);
    }
}
