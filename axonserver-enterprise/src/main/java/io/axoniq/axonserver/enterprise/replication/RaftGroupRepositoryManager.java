package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.RaftAdminGroup;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminContextRepository;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class RaftGroupRepositoryManager {

    private final ReplicationGroupMemberRepository raftGroupNodeRepository;
    private final ReplicationGroupContextRepository replicationGroupContextRepository;
    private final AdminReplicationGroupRepository adminReplicationGroupRepository;
    private final AdminContextRepository adminContextRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final AtomicReference<Map<String, String>> contextsCache = new AtomicReference<>();
    private final Map<String, Map<Role, Set<String>>> nodesPerRolePerReplicationGroup = new ConcurrentHashMap<>();
    private final Map<String, Role> rolePerReplicationGroup = new ConcurrentHashMap<>();

    public RaftGroupRepositoryManager(
            ReplicationGroupMemberRepository raftGroupNodeRepository,
            ReplicationGroupContextRepository replicationGroupContextRepository,
            AdminReplicationGroupRepository adminReplicationGroupRepository,
            AdminContextRepository adminContextRepository,
            MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.replicationGroupContextRepository = replicationGroupContextRepository;
        this.adminReplicationGroupRepository = adminReplicationGroupRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.adminContextRepository = adminContextRepository;
    }

    public Set<ReplicationGroupMember> getMyReplicationGroups() {
        return findByNodeName(messagingPlatformConfiguration.getName());
    }

    /**
     * @return the names of all contexts that have an event store on this node
     */
    public Set<String> storageContexts() {
        return filter(contextsCache(), RoleUtils::hasStorage);
    }

    private Set<String> filter(Map<String, String> contextReplicationGroup, Predicate<Role> roleFilter) {
        Set<String> contexts = new HashSet<>();
        contextReplicationGroup.forEach((context, replicationGroup) -> {
            if (roleFilter.test(myRole(replicationGroup))) {
                contexts.add(context);
            }
        });
        return contexts;
    }


    /**
     * Checks whether there is a given {@code context} within this node.
     *
     * @param context the context to check whether it exists within this node
     * @return {@code true} if contexts exists within this node, {@code false} otherwise
     */
    public boolean containsStorageContext(String context) {
        return storageContexts().contains(context);
    }

    private Map<String, String> refreshContextCache() {
        Map<String, String> contexts = raftGroupNodeRepository.findByNodeName(messagingPlatformConfiguration.getName())
                                                              .stream()
                                                              .map(ReplicationGroupMember::getGroupId)
                                                              .filter(n -> !RaftAdminGroup.isAdmin(n))
                                                              .flatMap(replicationGroup -> replicationGroupContextRepository
                                                                      .findByReplicationGroupName(replicationGroup)
                                                                      .stream())
                                                              .collect(Collectors
                                                                               .toMap(ReplicationGroupContext::getName,
                                                                                      ReplicationGroupContext::getReplicationGroupName));


        contextsCache.set(contexts);
        return contexts;
    }

    public Set<ReplicationGroupMember> findByGroupId(String groupId) {
        return raftGroupNodeRepository.findByGroupId(groupId);
    }

    public void delete(String groupId) {
        raftGroupNodeRepository.deleteAll(raftGroupNodeRepository.findByGroupId(groupId));
        nodesPerRolePerReplicationGroup.remove(groupId);
        rolePerReplicationGroup.remove(groupId);
        refreshContextCache();
    }

    public void update(String groupId, List<Node> nodes) {
        delete(groupId);
        nodes.forEach(n -> {
            ReplicationGroupMember jpaRaftGroupNode = new ReplicationGroupMember(groupId, n);
            raftGroupNodeRepository.save(jpaRaftGroupNode);
        });
        nodesPerRolePerReplicationGroup.remove(groupId);
        rolePerReplicationGroup.remove(groupId);
        refreshContextCache();
    }

    public Set<ReplicationGroupMember> findByNodeName(String nodeName) {
        return new HashSet<>(raftGroupNodeRepository.findByNodeName(nodeName));
    }

    public void prepareDeleteNodeFromReplicationGroup(String context, String node) {
        raftGroupNodeRepository.findByGroupIdAndNodeName(context, node)
                               .ifPresent(n -> {
                                   n.setPendingDelete(true);
                                   raftGroupNodeRepository.save(n);
                                   refreshContextCache();
                               });
    }

    public Set<String> contextsPerReplicationGroup(String replicationGroupName) {
        return adminReplicationGroupRepository
                .findByName(replicationGroupName)
                .map(replicationGroup -> replicationGroup.getContexts()
                                                         .stream()
                                                         .map(AdminContext::getName)
                                                         .collect(Collectors.toSet())
                ).orElseGet(() -> replicationGroupContextRepository.findByReplicationGroupName(replicationGroupName)
                                                                   .stream()
                                                                   .map(ReplicationGroupContext::getName)
                                                                   .collect(Collectors.toSet()));
    }

    /**
     * Checks if the context has nodes is a lower tier.
     *
     * @param context the name of context
     * @return true if the replication group has nodes is a lower tier
     */
    public boolean hasLowerTier(String context) {
        return !nextTierEventStores(context).isEmpty();
    }


    /**
     * Finds the nodes for the next tier in this context.
     *
     * @param context the name of context
     * @return a set of nodes for the next tier
     */
    public Set<String> nextTierEventStores(String context) {
        String replicationGroup = replicationGroup(context);
        Role nextTierRole = getNextTearRole(replicationGroup);
        if (Role.UNRECOGNIZED.equals(nextTierRole)) {
            return Collections.emptySet();
        }

        return nodesPerRolePerReplicationGroup.computeIfAbsent(replicationGroup, this::initNodesPerRole)
                                              .getOrDefault(nextTierRole, Collections.emptySet());
    }

    @EventListener
    public void on(ContextEvents.ContextCreated contextCreated) {
        contextsCache().put(contextCreated.context(), contextCreated.replicationGroup());
    }

    private Map<String, String> contextsCache() {
        return contextsCache.updateAndGet(old -> {
            if (old == null) {
                return refreshContextCache();
            }
            return old;
        });
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        if (contextsCache.get() != null) {
            contextsCache.get().remove(contextDeleted.context());
        }
    }

    private String replicationGroup(String context) {
        String replicationGroup = contextsCache().get(context);
        if (replicationGroup != null) {
            return replicationGroup;
        }

        replicationGroup = adminContextRepository.findById(context)
                                                 .map(adminContext -> adminContext.getReplicationGroup()
                                                                                  .getName())
                                                 .orElse(null);
        if (replicationGroup != null) {
            return replicationGroup;
        }

        throw new RuntimeException(context + ": not found in any replication group");
    }

    private Map<Role, Set<String>> initNodesPerRole(String replicationGroup) {
        Map<Role, Set<String>> nodesPerRole = new EnumMap<>(Role.class);
        raftGroupNodeRepository.findByGroupId(replicationGroup)
                               .forEach(n -> nodesPerRole.computeIfAbsent(n.getRole(), r -> new HashSet<>())
                                                         .add(n.getNodeName()));
        return nodesPerRole;
    }

    private Role getNextTearRole(String replicationGroup) {
        Role myRole = myRole(replicationGroup);
        switch (myRole) {
            case PRIMARY:
            case MESSAGING_ONLY:
                return Role.SECONDARY;
            default:
                return Role.UNRECOGNIZED;
        }
    }

    private Role myRole(String replicationGroup) {
        return rolePerReplicationGroup.computeIfAbsent(replicationGroup, c ->
                raftGroupNodeRepository.findByGroupIdAndNodeName(c, messagingPlatformConfiguration.getName())
                                       .map(ReplicationGroupMember::getRole)
                                       .orElse(Role.UNRECOGNIZED));
    }


    /**
     * Returns the tier of the current node for a context.
     *
     * @param context the name of the context
     * @return the number of the tier
     */
    public int tier(String context) {

        Role myRole = myRole(replicationGroup(context));
        switch (myRole) {
            case PRIMARY:
            case MESSAGING_ONLY:
                return 0;
            case SECONDARY:
                return 1;
            default:
                return -1;
        }
    }
}
