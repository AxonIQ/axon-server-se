package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.ContextUpdate;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private final ApplicationEventPublisher eventPublisher;
    private final ModelVersionController modelVersionController;

    public ContextController(
            EntityManager entityManager,
            ClusterController clusterController,
            ApplicationEventPublisher eventPublisher,
            ModelVersionController modelVersionController) {
        this.entityManager = entityManager;
        this.clusterController = clusterController;
        this.eventPublisher = eventPublisher;
        this.modelVersionController = modelVersionController;
    }

    public Stream<Context> getContexts() {
        return entityManager.createQuery("select c from Context c", Context.class)
                            .getResultList()
                            .stream();
    }

    @Transactional
    public ContextEvents.NodeRolesUpdated updateNodeRoles(String contextName, String nodeName,
                                                          boolean storage, boolean messaging,
                                                          boolean proxied) {
        if( ! storage && ! messaging) return deleteNodeFromContext(contextName, nodeName, proxied);
        Context context = entityManager.find(Context.class, contextName);
        if (context == null) {
            throw new IllegalArgumentException("Context does not exist: " + contextName);
        }
        ContextClusterNode contextClusterNode = context.getMember(nodeName);
        if (contextClusterNode != null) {
            contextClusterNode.setStorage(storage);
            contextClusterNode.setMessaging(messaging);
        } else {
            ClusterNode clusterNode = entityManager.find(ClusterNode.class, nodeName);
            if (clusterNode == null) {
                throw new IllegalArgumentException("ClusterNode does not exist: " + nodeName);
            }
            contextClusterNode = new ContextClusterNode(context, clusterNode);
            contextClusterNode.setStorage(storage);
            contextClusterNode.setMessaging(messaging);
        }
        entityManager.flush();
        return new ContextEvents.NodeRolesUpdated(contextName, new NodeRoles(contextClusterNode), proxied);
    }

    @Transactional
    public ContextEvents.ContextDeleted deleteContext(String name, boolean proxied) {
        if (Topology.DEFAULT_CONTEXT.equals(name)) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_DEFAULT,
                                                 "Not allowed to delete default context");
        }
        Context context = entityManager.find(Context.class, name);
        entityManager.remove(context);
        entityManager.flush();
        return new ContextEvents.ContextDeleted(name, proxied);
    }

    @Transactional
    public ContextEvents.NodeRolesUpdated deleteNodeFromContext(String contextName, String nodeName,
                                                                      boolean proxied) {
        Context context = entityManager.find(Context.class, contextName);
        if (context == null) {
            throw new IllegalArgumentException("Context does not exist: " + contextName);
        }
        ContextClusterNode contextClusterNode = context.getMember(nodeName);
        if (contextClusterNode != null) {
            entityManager.remove(contextClusterNode);
            entityManager.flush();
        }
        return new ContextEvents.NodeRolesUpdated(contextName, new NodeRoles(nodeName, false, false), proxied);
    }



    @Transactional
    public ContextEvents.ContextCreated addContext(String name, List<NodeRoles> nodes, boolean proxied) {
        Context contextJPA = new Context(name);
        nodes.forEach(n -> addToNode(n, contextJPA));
        entityManager.persist(contextJPA);
        entityManager.flush();
        return new ContextEvents.ContextCreated(name, nodes, proxied);
    }

    private void addToNode(NodeRoles n, Context contextJPA) {
        entityManager.find(ClusterNode.class, n.getName()).addContext(contextJPA, n.isStorage(), n.isMessaging());
    }


    @Transactional
    public Iterable<ContextEvents.BaseContextEvent> update(ContextUpdate context) {
        switch (context.getAction()) {
            case MERGE_CONTEXT:
                List<NodeRoles> roles = context.getNodesList().stream().map(n -> new NodeRoles(n.getName(), n.getMessaging(), n.getStorage())).collect(
                        Collectors.toList());
                ContextEvents.ContextCreated event = addContext(context.getName(), roles, true);
                return Collections.singleton(event);
            case DELETE_CONTEXT:
                ContextEvents.ContextDeleted deleteEvent = deleteContext(context.getName(), true);
                return deleteEvent != null ? Collections.singleton(deleteEvent) : Collections.emptySet();
            case NODES_UPDATED:
                return context.getNodesList().stream().map(node ->
                                                                   updateNodeRoles(context.getName(), node.getName(),
                                                                                   node.getStorage(),
                                                                                   node.getMessaging(), true)
                ).collect(Collectors.toSet());
            case UNRECOGNIZED:
                break;
        }
        modelVersionController.updateModelVersion(ClusterNode.class, context.getGeneration());
        return Collections.emptySet();
    }

    //@EventListener
    @Transactional
    public void on(ClusterEvents.AxonServerInstanceConnected axonHubInstanceConnected) {
        String node = axonHubInstanceConnected.getRemoteConnection().getClusterNode().getName();
        ClusterNode clusterNode = entityManager.find(ClusterNode.class, node);
        if (clusterNode == null) {
            return;
        }
        Set<String> contextsToRemove = clusterNode.getContextNames();
        contextsToRemove.removeIf(c -> contains(c, axonHubInstanceConnected.getContextsList()));
        contextsToRemove.forEach(context -> deleteNodeFromContext(context, node, true));
        List<ContextEvents.BaseContextEvent> events = new ArrayList<>();
        axonHubInstanceConnected.getContextsList().forEach(context -> {
            Context context1 = entityManager.find(Context.class, context.getName());
            if (context1 != null) {
                events.add(updateNodeRoles(context.getName(), node, context.getStorage(), context.getMessaging(), true));
            } else {
                events.add(addContext(context.getName(), Collections.singletonList(new NodeRoles(node, context.getMessaging(), context.getStorage())), true));
            }
        });

        events.forEach(eventPublisher::publishEvent);
    }

    private boolean contains(String c, List<ContextRole> contextsList) {
        for (ContextRole contextRole : contextsList) {
            if( contextRole.getName().equals(c)) return true;
        }

        return false;
    }

    public Context getContext(String contextName){
        return entityManager.find(Context.class, contextName);
    }

    public void canDeleteContext(String name) {
        Context context = getContext(name);
        if( context == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, name + " not found");
        }
        for (ContextClusterNode node : context.getAllNodes()) {
            if( ! clusterController.isActive(node.getClusterNode().getName())) {
                throw new MessagingPlatformException(ErrorCode.AXONSERVER_NODE_NOT_CONNECTED, node.getClusterNode().getName() + " not connected, cannot update context " + name);
            }
        }
    }

    public void canUpdateContext(String name, String node) {
        Context context = getContext(name);
        if( context == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, name + " not found");
        }

        if( !clusterController.isActive(node) ) {
            throw new MessagingPlatformException(ErrorCode.AXONSERVER_NODE_NOT_CONNECTED, node + " not connected, cannot update context " + name);
        }
    }

    public void canAddContext(List<NodeRoles> nodes) {
        for (NodeRoles node : nodes) {
            if( ! clusterController.isActive(node.getName())) {
                throw new MessagingPlatformException(ErrorCode.AXONSERVER_NODE_NOT_CONNECTED, node.getName() + " not connected, cannot create context");
            }

        }

    }
}
