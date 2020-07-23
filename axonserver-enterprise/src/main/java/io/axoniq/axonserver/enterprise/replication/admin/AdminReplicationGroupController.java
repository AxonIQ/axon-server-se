package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingService;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminContextRepository;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * Controls the changes to replication group information for the part that is stored in the _admin nodes. Admin nodes
 * maintain a list of all the replication groups and their members and contexts.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class AdminReplicationGroupController {

    private final Logger logger = LoggerFactory.getLogger(AdminReplicationGroupController.class);

    private final AdminReplicationGroupRepository replicationGroupRepository;
    private final AdminContextRepository contextRepository;
    private final AdminApplicationController applicationController;
    private final UserController userController;
    private final ClusterController clusterController;
    private final AdminProcessorLoadBalancingService processorLoadBalancingService;
    private final ApplicationEventPublisher applicationEventPublisher;

    /**
     * Instantiate the AdminReplicationGroupController.
     *
     * @param replicationGroupRepository    repository of {@link AdminReplicationGroup} entries.
     * @param contextRepository             repository of {@link AdminContext} entries.
     * @param applicationController         controls all applications
     * @param userController                controls all users
     * @param clusterController             controls information on cluster nodes
     * @param processorLoadBalancingService controls all load balancing strategies
     * @param applicationEventPublisher     publisher to publish internal events
     */
    public AdminReplicationGroupController(
            AdminReplicationGroupRepository replicationGroupRepository,
            AdminContextRepository contextRepository,
            AdminApplicationController applicationController,
            UserController userController,
            ClusterController clusterController,
            AdminProcessorLoadBalancingService processorLoadBalancingService,
            ApplicationEventPublisher applicationEventPublisher) {
        this.replicationGroupRepository = replicationGroupRepository;
        this.contextRepository = contextRepository;
        this.applicationController = applicationController;
        this.userController = userController;
        this.clusterController = clusterController;
        this.processorLoadBalancingService = processorLoadBalancingService;

        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Updates the information stored in the ADMIN tables regarding a replication group and its members to
     * reflect the passed configuration.
     *
     * @param replicationGroupConfiguration the updated context configuration
     */
    @Transactional
    public AdminReplicationGroup updateReplicationGroup(ReplicationGroupConfiguration replicationGroupConfiguration) {
        Optional<AdminReplicationGroup> optionalReplicationGroup = replicationGroupRepository.findByName(
                replicationGroupConfiguration.getReplicationGroupName());
        if (!replicationGroupConfiguration.getPending() && replicationGroupConfiguration.getNodesCount() == 0) {
            optionalReplicationGroup.ifPresent(this::unregisterReplicationGroup);
            return null;
        }


        AdminReplicationGroup replicationGroup = optionalReplicationGroup.orElseGet(() -> {
            AdminReplicationGroup c = new AdminReplicationGroup();
            c.setName(replicationGroupConfiguration.getReplicationGroupName());
            return replicationGroupRepository.save(c);
        });
        replicationGroup.setChangePending(replicationGroupConfiguration.getPending());
        Map<String, ClusterNode> currentNodes = new HashMap<>();
        replicationGroup.getMembers().forEach(n -> currentNodes.put(n.getClusterNode().getName(), n.getClusterNode()));
        Map<String, NodeInfoWithLabel> newNodes = new HashMap<>();
        replicationGroupConfiguration.getNodesList().forEach(n -> newNodes.put(n.getNode().getNodeName(), n));

        Map<String, ClusterNode> clusterInfoMap = new HashMap<>();
        for (NodeInfoWithLabel nodeInfo : replicationGroupConfiguration.getNodesList()) {
            String nodeName = nodeInfo.getNode().getNodeName();
            ClusterNode clusterNode = clusterController.getNode(nodeName);
            if (clusterNode == null) {
                logger.debug("{}: Creating new connection to {}",
                             replicationGroupConfiguration.getReplicationGroupName(),
                             nodeInfo.getNode().getNodeName());
                clusterNode = clusterController.addConnection(nodeInfo.getNode());
            }
            clusterInfoMap.put(nodeName, clusterNode);
        }

        currentNodes.forEach((node, clusterNode) -> {
            if (!newNodes.containsKey(node)) {
                logger.debug("{}: Node not in new configuration {}",
                             replicationGroupConfiguration.getReplicationGroupName(),
                             node);
                clusterNode.removeReplicationGroup(replicationGroup);
            }
        });
        newNodes.forEach((node, nodeInfo) -> {
            if (!currentNodes.containsKey(node)) {
                logger.debug("{}: Node not in current configuration {}",
                             replicationGroupConfiguration.getReplicationGroupName(),
                             node);
                clusterInfoMap.get(node).addReplicationGroup(replicationGroup, nodeInfo.getLabel(), nodeInfo.getRole());
            }
        });
        applicationEventPublisher.publishEvent(new ClusterEvents.ReplicationGroupUpdated(replicationGroup.getName()));
        return replicationGroup;
    }


    /**
     * Registers an event listener that listens for {@link ClusterEvents.ReplicationGroupDeleted}
     * events.
     * This event occurs when the current node is no longer an admin node, in which case the admin tables for the
     * context can be cleared.
     *
     * @param event only used for registering the listener
     */
    @EventListener
    @Transactional
    public void on(ClusterEvents.ReplicationGroupDeleted event) {
        if (isAdmin(event.replicationGroup())) {
            unregisterAllAdminData();
        }
    }

    /**
     * Handles events of type {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.DeleteNodeFromReplicationGroupRequested}.
     * Updates the status of given node for all contexts in the replication group to pending delete, so platform
     * service
     * will no longer select this node as a target for new client connections.
     *
     * @param prepareDeleteNodeFromContext the event
     */
    @EventListener
    @Transactional
    public void on(ClusterEvents.DeleteNodeFromReplicationGroupRequested prepareDeleteNodeFromContext) {
        replicationGroupRepository.findByName(prepareDeleteNodeFromContext.replicationGroup())
                                  .ifPresent(c -> {
                                      c.getMember(prepareDeleteNodeFromContext.node()).ifPresent(ccn -> {
                                          ccn.setPendingDelete(true);
                                          replicationGroupRepository.save(c);
                                      });
                                  });
    }

    @Transactional
    public void unregisterAllAdminData() {
        logger.debug("Clearing admin replication group definitions");
        replicationGroupRepository.deleteAll();
        userController.deleteAll();
        applicationController.clearApplications();
        contextRepository.deleteAll();
        processorLoadBalancingService.deleteAll();
    }

    private void unregisterReplicationGroup(AdminReplicationGroup replicationGroup) {
        Set<AdminContext> contextsToDelete = new HashSet<>(replicationGroup.getContexts());
        replicationGroupRepository.delete(replicationGroup);
        contextsToDelete.forEach(context -> {
            userController.removeRolesForContext(context.getName());
            applicationController.removeRolesForContext(context.getName());
            processorLoadBalancingService.deleteByContext(context.getName());
        });
    }

    /**
     * Retrieves a list of replication groups defined in admin tables.
     *
     * @return list of replication groups defined in admin tables
     */
    public List<AdminReplicationGroup> findAll() {
        return replicationGroupRepository.findAll();
    }

    /**
     * Retrieves a list of all the nodes that are member of the given replication group name. Returns an empty list if
     * the replication group is not found.
     *
     * @param replicationGroupName the replication group name
     * @return collection of node names
     */
    public Collection<String> getNodeNames(String replicationGroupName) {
        return replicationGroupRepository.findByName(replicationGroupName)
                                         .map(AdminReplicationGroup::getMemberNames)
                                         .orElse(Collections.emptyList());
    }

    /**
     * Registers a new context in the admin tables.
     *
     * @param replicationGroupName the name of the replication group
     * @param contextName          the name of the context
     * @param metaDataMap          meta data for the context
     */
    @Transactional
    public void registerContext(String replicationGroupName, String contextName, Map<String, String> metaDataMap) {
        replicationGroupRepository.findByName(replicationGroupName).ifPresent(replicationGroup -> {
            AdminContext context = new AdminContext(contextName);
            context.setMetaDataMap(metaDataMap);
            context.setReplicationGroup(replicationGroup);
            replicationGroup.getContexts().add(context);
            applicationEventPublisher.publishEvent(new ClusterEvents.ReplicationGroupUpdated(replicationGroupName));
        });
    }

    /**
     * Registers contexts in the admin tables.
     *
     * @param contexts definition of the replication group and its contexts
     */
    @Transactional
    public void registerContexts(ReplicationGroupContexts contexts) {
        for (io.axoniq.axonserver.grpc.internal.Context context : contexts.getContextList()) {
            registerContext(contexts.getReplicationGroupName(), context.getContextName(), context.getMetaDataMap());
        }
    }

    /**
     * Removes all contexts from the admin tables.
     */
    @Transactional
    public void unregisterAllContexts() {
        contextRepository.deleteAll();
    }

    public Optional<AdminReplicationGroup> findByName(String name) {
        return replicationGroupRepository.findByName(name);
    }

    /**
     * Removes the registration of the context from the admin table.
     *
     * @param replicationGroupName the name of the replication group
     * @param contextName          the name of the context
     */
    @Transactional
    public void unregisterContext(String replicationGroupName, String contextName) {
        contextRepository.findById(contextName).ifPresent(context -> context.getReplicationGroup()
                                                                            .removeContext(context));
        applicationController.removeRolesForContext(contextName);
        userController.removeRolesForContext(contextName);
        processorLoadBalancingService.deleteByContext(contextName);
        applicationEventPublisher.publishEvent(new ClusterEvents.ReplicationGroupUpdated(replicationGroupName));
    }

    /**
     * Handles context configuration request that may be sent by install snapshot from pre 4.4 versions of Axon Server.
     *
     * @param contextConfiguration pre 4.4 request containing the context information
     */
    @Transactional
    public void updateReplicationGroup(ContextConfiguration contextConfiguration) {
        ReplicationGroupConfiguration replicationGroupConfiguration = ReplicationGroupConfiguration.newBuilder()
                                                                                                   .setReplicationGroupName(
                                                                                                           contextConfiguration
                                                                                                                   .getContext())
                                                                                                   .addAllNodes(
                                                                                                           contextConfiguration
                                                                                                                   .getNodesList())
                                                                                                   .build();
        AdminReplicationGroup replicationGroup = updateReplicationGroup(replicationGroupConfiguration);

        AdminContext context = contextRepository.findById(contextConfiguration.getContext())
                                                .orElseGet(() -> replicationGroup
                                                        .addContext(contextConfiguration.getContext()));

        context.setMetaDataMap(contextConfiguration.getMetaDataMap());
    }

    /**
     * Registers the information of a replication group in the admin tables.
     *
     * @param replicationGroupConfiguration configuration of the new replication group
     */
    @Transactional
    public void registerReplicationGroup(ReplicationGroupConfiguration replicationGroupConfiguration) {
        AdminReplicationGroup adminReplicationGroup = new AdminReplicationGroup();
        adminReplicationGroup.setName(replicationGroupConfiguration.getReplicationGroupName());

        for (NodeInfoWithLabel nodeInfo : replicationGroupConfiguration.getNodesList()) {
            String nodeName = nodeInfo.getNode().getNodeName();
            ClusterNode clusterNode = clusterController.getNode(nodeName);
            clusterNode.addReplicationGroup(adminReplicationGroup, nodeInfo.getLabel(), nodeInfo.getRole());
        }
        replicationGroupRepository.save(adminReplicationGroup);
    }

    public Set<String> contextsPerReplicationGroup(String replicationGroup) {
        return replicationGroupRepository.findByName(replicationGroup)
                                         .map(c -> c.getContexts()
                                                    .stream()
                                                    .map(AdminContext::getName)
                                                    .collect(Collectors.toSet()))
                                         .orElse(Collections.emptySet());
    }
}
