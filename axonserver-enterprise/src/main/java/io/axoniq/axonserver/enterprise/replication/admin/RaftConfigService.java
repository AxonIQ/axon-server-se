package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Request changes to the Axon Server configuration, to be handled through Raft.
 * Operations will be executed on the leader of the _admin context.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface RaftConfigService {

    /**
     * Create a new replication group, with the specified nodes as its members.
     *
     * @param name    the name of the replication group
     * @param members the nodes that are member of the replication group and their roles
     */
    void createReplicationGroup(String name, Collection<ReplicationGroupMember> members);

    /**
     * Deletes the named replication group. If preserveEventStore is true, the event stores for
     * the contexts that are member of the replication group are not deleted.
     *
     * @param name               the name of the replication group
     * @param preserveEventStore true to preserve the event data for the contexts in the replication group
     */
    void deleteReplicationGroup(String name, boolean preserveEventStore);

    /**
     * Adds a node to a context. The node information should already be present.
     *
     * @param replicationGroup the replicationGroup of the replication group
     * @param node             the replicationGroup of the node
     * @param role             the role of the node in the context
     * @return completable future that completes when node is added and up-to-date
     */
    CompletableFuture<Void> addNodeToReplicationGroup(String replicationGroup, String node, Role role);

    /**
     * Deletes a member from a replication group.
     *
     * @param name the name of the replication group
     * @param node the name of the node
     */
    void deleteNodeFromReplicationGroup(String name, String node);

    /**
     * Deletes a context from all nodes where it is present.
     *
     * @param name the name of the context
     */
    void deleteContext(String name, boolean preserveEventStore);

    /**
     * Create a new context and adds all specified nodes to the context. Node information for the nodes must be present
     * (nodes must have joined).
     *
     * @param replicationGroup replication group where context should be added
     * @param context          the name of the context
     * @param metaData         additional information on the context to add
     */
    CompletableFuture<Void> addContext(String replicationGroup, String context, Map<String, String> metaData);

    /**
     * Handles a join request from another node. Can only be executed on the leader of the _admin context.
     * Adds the new node to all specified contexts. If no contexts are specified it adds the node to all contexts.
     * If list of contexts contains unknown contexts, they will be created.
     * First sends the addNode request to the leader of the context, then sends the new configuration of the context as an
     * entry to the _admin context, so it can update the master configuration.
     *
     * If a context has a large eventstore it may take some time to complete.
     *
     * @param nodeInfo Node information on the new node
     * @return
     */
    UpdateLicense join(NodeInfo nodeInfo);

    /**
     * Initialize a node with specified contexts and the _admin context. Node becomes leader for all specified contexts.
     * @param contexts
     */
    void init(List<String> contexts);

    /**
     * Creates or updates an application to be used for access control.
     * If the application does not exist and it does not contain a token it will generate a random token.
     * @param application the application and roles per context
     * @return the application with generated token (if generated) in a completable future
     */
    Application updateApplication(Application application);

    /**
     * Generates a new token for an existing application.
     * @param application the application
     * @return the application with a new generated token
     */
    Application refreshToken(Application application);

    /**
     * Creates or updates a user.
     *
     * @param user the user and its roles
     */
    void updateUser(User user);

    /**
     * Updates the load balancing strategy to use for a processor in a context
     *
     * @param processorLBStrategy the new load balancing strategy to use for a processor in a context
     */
    void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy);

    /**
     * Deletes a user.
     *
     * @param user the user to delete
     */
    void deleteUser(User user);

    /**
     * Deletes an application.
     * @param application the application to delete
     */
    void deleteApplication(Application application);

    /**
     * Deletes a node from the configuration. Deletes the node from all contexts where it is member of.
     * @param name
     */
    void deleteNode(String name);

    /**
     * Deletes a node from the configuration. Only allowed when no more contexts on node.
     *
     * @param name
     */
    void deleteNodeIfEmpty(String name);
}
