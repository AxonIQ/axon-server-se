package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Request changes to the Axon Server configuration, to be handled through Raft.
 * Operations will be executed on the leader of the _admin context.
 *
 * @author Marc Gathier
 */
public interface RaftConfigService {

    /**
     * Adds a node to a context. The node information should already be present.
     * @param name the name of the context
     * @param node the name of the node
     * @param role the role of the node in the context
     * @return
     */
    CompletableFuture<Void> addNodeToContext(String name, String node, Role role);

    /**
     * Deletes a context from all nodes where it is present.
     * @param name the name of the context
     */
    void deleteContext(String name);

    /**
     * Removes a node from a context. The log directory for the context on the deleted node will be removed, event store is
     * not removed.
     * @param name the name of the context
     * @param node the name of the node
     */
    void deleteNodeFromContext(String name, String node);

    /**
     * Create a new context and adds all specified nodes to the context. Node information for the nodes must be present (nodes must have joined).
     * @param context definition of the context
     */
    void addContext(Context context);

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
     * @param user the user and its roles
     */
    void updateUser(User user);

    void updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy);

    void deleteLoadBalancingStrategy(LoadBalanceStrategy build);

    void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy);

    /**
     * Deletes a user.
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
