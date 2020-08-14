package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.access.application.ReplicationGroupApplicationController;
import io.axoniq.axonserver.access.user.ReplicationGroupUserController;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContexts;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Controls storage of replication group information for the information that is stored on each member of the
 * replication group.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class ReplicationGroupController {

    private final ReplicationGroupMemberRepository replicationGroupMemberRepository;
    private final ReplicationGroupContextRepository replicationGroupContextRepository;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ReplicationGroupApplicationController applicationController;
    private final ReplicationGroupUserController userController;
    private final ReplicationGroupProcessorLoadBalancingRepository processorLoadBalancingRepository;
    private final Map<String, String> replicationGroupPerContext = new ConcurrentHashMap<>();
    private final String name;

    /**
     * Constructor.
     *
     * @param replicationGroupMemberRepository  repository of members of a replication group
     * @param replicationGroupContextRepository repository of contexts of a replication group
     * @param applicationEventPublisher         publisher for application events
     * @param applicationController             controls the application information for a replication group
     * @param userController                    controls the user information for a replication group
     * @param processorLoadBalancingRepository  controls the processor load balancing information for a replication
     *                                          group
     * @param messagingPlatformConfiguration    configuration of the Axon Server node
     */
    public ReplicationGroupController(
            ReplicationGroupMemberRepository replicationGroupMemberRepository,
            ReplicationGroupContextRepository replicationGroupContextRepository,
            ApplicationEventPublisher applicationEventPublisher,
            ReplicationGroupApplicationController applicationController,
            ReplicationGroupUserController userController,
            ReplicationGroupProcessorLoadBalancingRepository processorLoadBalancingRepository,
            MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.replicationGroupMemberRepository = replicationGroupMemberRepository;
        this.replicationGroupContextRepository = replicationGroupContextRepository;
        this.applicationEventPublisher = applicationEventPublisher;
        this.applicationController = applicationController;
        this.userController = userController;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
        this.name = messagingPlatformConfiguration.getName();
    }

    /**
     * Stores the information for a newly created context.
     *
     * @param replicationGroupName the name of the replication group
     * @param contextName          the name of the context
     * @param metaDataMap          meta data for the context
     */
    @Transactional
    public void addContext(String replicationGroupName, String contextName, Map<String, String> metaDataMap) {
        storeContext(replicationGroupName, contextName, metaDataMap);
        applicationEventPublisher.publishEvent(new ContextEvents.ContextCreated(contextName,
                                                                                replicationGroupName,
                                                                                getMyRole(replicationGroupName),
                                                                                0L,
                                                                                0L));
    }

    private void storeContext(String replicationGroupName, String contextName, Map<String, String> metaDataMap) {
        ReplicationGroupContext context = new ReplicationGroupContext();
        context.setMetaDataMap(metaDataMap);
        context.setName(contextName);
        context.setReplicationGroupName(replicationGroupName);
        replicationGroupContextRepository.save(context);
    }

    /**
     * Retrieves a list of context objects for the replication group.
     *
     * @param replicationGroup the name of the replication group
     * @return a list of context objects for the replication group
     */
    public List<ReplicationGroupContext> findContextsByReplicationGroupName(String replicationGroup) {
        return replicationGroupContextRepository.findByReplicationGroupName(replicationGroup);
    }

    /**
     * Merges the existing context information with new context information received from the replication leader.
     *
     * @param contexts new context information for the replication group
     * @param role     role of the node in the replication group
     */
    @Transactional
    public void merge(ReplicationGroupContexts contexts, Role role) {
        Set<String> current = replicationGroupContextRepository
                .findByReplicationGroupName(contexts.getReplicationGroupName())
                .stream()
                .map(ReplicationGroupContext::getName)
                .collect(Collectors.toSet());

        contexts.getContextList().forEach(context -> {
            if (current.contains(context.getContextName())) {
                ReplicationGroupContext existingContext = replicationGroupContextRepository
                        .getOne(context.getContextName());
                existingContext.setMetaDataMap(context.getMetaDataMap());
                current.remove(context.getContextName());
            } else {
                storeContext(contexts.getReplicationGroupName(), context.getContextName(), context.getMetaDataMap());
                applicationEventPublisher.publishEvent(new ContextEvents.ContextCreated(context.getContextName(),
                                                                                        contexts.getReplicationGroupName(),
                                                                                        role,
                                                                                        context.getFirstEventToken(),
                                                                                        context.getFirstSnapshotToken()));
            }
        });

        current.forEach(context -> {
            replicationGroupContextRepository.deleteById(context);
            applicationEventPublisher.publishEvent(new ContextEvents.ContextDeleted(context,
                                                                                    contexts.getReplicationGroupName(),
                                                                                    false));
        });
    }

    /**
     * Deletes a context.
     *
     * @param context            the name of the context
     * @param preserveEventStore save the event store for this context
     */
    @Transactional
    public void deleteContext(String context, String replicationGroup, boolean preserveEventStore) {
        applicationController.deleteByContext(context);
        userController.deleteByContext(context);
        processorLoadBalancingRepository.deleteAllByProcessorContext(context);
        replicationGroupContextRepository.findById(context).ifPresent(replicationGroupContextRepository::delete);
        applicationEventPublisher.publishEvent(new ContextEvents.ContextDeleted(context,
                                                                                replicationGroup,
                                                                                preserveEventStore));
        replicationGroupPerContext.remove(context);
    }

    /**
     * Deletes all contexts in a replication group.
     *
     * @param replicationGroup   the name of the replication group
     * @param preserveEventStore save the event store for the contexts in this replication group
     */
    @Transactional
    public void deleteReplicationGroup(String replicationGroup, boolean preserveEventStore) {
        getContextNames(replicationGroup).forEach(context -> deleteContext(context,
                                                                           replicationGroup,
                                                                           preserveEventStore));
    }

    /**
     * Retrieves the names of all contexts in a replication group.
     *
     * @param replicationGroup the name of the replication group
     * @return list of context names
     */
    public List<String> getContextNames(String replicationGroup) {
        return replicationGroupContextRepository.findByReplicationGroupName(replicationGroup).stream().map(
                ReplicationGroupContext::getName).collect(
                Collectors.toList());
    }

    /**
     * Retrieves the role of the current node in the replication group.
     *
     * @param replicationGroupName the name of the replication group
     * @return the role of the current node in the replication group
     */
    public Role getMyRole(String replicationGroupName) {
        return replicationGroupMemberRepository.findByGroupIdAndNodeName(replicationGroupName, name)
                                               .map(ReplicationGroupMember::getRole)
                                               .orElse(null);
    }

    /**
     * Returns the name of the replication group containing the given context.
     *
     * @param context the name of the context
     * @return the name of the replication group
     */
    public Optional<String> findReplicationGroupByContext(String context) {
        String replicationGroup = replicationGroupPerContext.get(context);
        if (replicationGroup != null) {
            return Optional.of(replicationGroup);
        }


        Optional<String> replicationGroupInDb = replicationGroupContextRepository.findById(context)
                                                                                 .map(ReplicationGroupContext::getReplicationGroupName);
        replicationGroupInDb.ifPresent(r -> replicationGroupPerContext.put(context, r));
        return replicationGroupInDb;
    }
}
