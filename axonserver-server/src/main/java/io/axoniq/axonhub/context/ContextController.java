package io.axoniq.axonhub.context;

import io.axoniq.axonhub.ClusterEvents;
import io.axoniq.axonhub.ContextEvents;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.context.jpa.Context;
import io.axoniq.axonhub.context.jpa.ContextClusterNode;
import io.axoniq.axonhub.exception.ErrorCode;
import io.axoniq.axonhub.exception.MessagingPlatformException;
import io.axoniq.axonhub.internal.grpc.ContextRole;
import io.axoniq.axonhub.internal.grpc.ContextUpdate;
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

    public static final String DEFAULT = "default";

    private final EntityManager entityManager;
    private final ApplicationEventPublisher eventPublisher;

    public ContextController(
            EntityManager entityManager, ApplicationEventPublisher eventPublisher) {
        this.entityManager = entityManager;
        this.eventPublisher = eventPublisher;
    }

    public Stream<Context> getContexts() {
        return entityManager.createQuery("select c from Context c", Context.class)
                            .getResultList()
                            .stream();
    }

    @Transactional
    public ContextEvents.NodeAddedToContext addNodeToContext(String contextName, String nodeName,
                                                             boolean storage,boolean messaging,
                                                             boolean proxied) {
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
        return new ContextEvents.NodeAddedToContext(contextName, new NodeRoles(contextClusterNode), proxied);
    }

    @Transactional
    public ContextEvents.ContextDeleted deleteContext(String name, boolean proxied) {
        if (DEFAULT.equals(name)) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_DEFAULT,
                                                 "Not allowed to delete default context");
        }
        Context context = entityManager.find(Context.class, name);
        entityManager.remove(context);
        entityManager.flush();
        return new ContextEvents.ContextDeleted(name, proxied);
    }

    @Transactional
    public ContextEvents.NodeDeletedFromContext deleteNodeFromContext(String contextName, String nodeName,
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
        return new ContextEvents.NodeDeletedFromContext(contextName, nodeName, proxied);
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
            case ADD_NODES:
                return context.getNodesList().stream().map(node ->
                                                                   addNodeToContext(context.getName(), node.getName(),
                                                                                    node.getStorage(),
                                                                                    node.getMessaging(), true)
                ).collect(Collectors.toSet());
            case DELETE_NODES:
                return context.getNodesList().stream().map(node ->
                                                                   deleteNodeFromContext(context.getName(), node.getName(), true)
                ).collect(Collectors.toSet());
            case UNRECOGNIZED:
                break;
        }
        return Collections.emptySet();
    }

    @EventListener
    @Transactional
    public void on(ClusterEvents.AxonHubInstanceConnected axonHubInstanceConnected) {
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
                events.add(addNodeToContext(context.getName(), node, context.getStorage(), context.getMessaging(), true));
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
}
