package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Manages updates in context configuration as they are stored for the ADMIN nodes.
 * @author Marc Gathier
 * @since 4.0
 */
@Controller
public class ContextController {
    private final Logger logger = LoggerFactory.getLogger(ContextController.class);
    private final ContextRepository contextRepository;
    private final ClusterController clusterController;

    /**
     * Constructor for the context controller.
     *
     * @param contextRepository for access to stored context data
     * @param clusterController for access to cluster information
     */
    public ContextController(
            ContextRepository contextRepository,
            ClusterController clusterController) {
        this.contextRepository = contextRepository;
        this.clusterController = clusterController;
    }

    /**
     * Get a stream of all contexts.
     *
     * @return stream of all contexts.
     */
    public Stream<Context> getContexts() {
        return contextRepository.findAll()
                                .stream();
    }

    /**
     * Returns a context based on its name. Returns null if context not found.
     * @param contextName the name of the context
     * @return the context or null
     */
    public Context getContext(String contextName) {
        return contextRepository.findById(contextName).orElse(null);
    }

    /**
     * Updates the information stored in the ADMIN tables regarding a context and its members to
     * reflect the passed configuration.
     * @param contextConfiguration the updated context configuration
     */
    @Transactional
    public void updateContext(ContextConfiguration contextConfiguration) {
        Optional<Context> optionalContext = contextRepository.findById(contextConfiguration.getContext());
        if( ! contextConfiguration.getPending() && contextConfiguration.getNodesCount() == 0) {
            optionalContext.ifPresent(contextRepository::delete);
            return;
        }


        Context context = optionalContext.orElseGet(() -> contextRepository
                .save(new Context(contextConfiguration.getContext())));
        context.changePending(contextConfiguration.getPending());
        Map<String, ClusterNode> currentNodes = new HashMap<>();
        context.getNodes().forEach(n -> currentNodes.put(n.getClusterNode().getName(), n.getClusterNode()) );
        Map<String, NodeInfoWithLabel> newNodes = new HashMap<>();
        contextConfiguration.getNodesList().forEach(n -> newNodes.put(n.getNode().getNodeName(), n));

        Map<String, ClusterNode> clusterInfoMap = new HashMap<>();
        for (NodeInfoWithLabel nodeInfo : contextConfiguration.getNodesList()) {
            String nodeName = nodeInfo.getNode().getNodeName();
            ClusterNode clusterNode = clusterController.getNode(nodeName);
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
                clusterInfoMap.get(node).addContext(finalContext,
                                                    nodeInfo.getLabel(),
                                                    Role.forNumber(nodeInfo.getRole()));
            }
        });
    }

    @Transactional
    public void deleteContext(String context) {
        contextRepository.deleteById(context);
    }

    /**
     * Registers an event listener that listens for {@link io.axoniq.axonserver.enterprise.ContextEvents.AdminContextDeleted} events.
     * This event occurs when the current node is no longer an admin node, in which case the admin tables for the context can be cleared.
     * @param contextDeleted only used for registering the listener
     */
    @EventListener
    @Transactional
    public void on(ContextEvents.AdminContextDeleted contextDeleted) {
        deleteAll();
    }

    @EventListener
    @Transactional
    public void on(ContextEvents.PrepareDeleteNodeFromContext prepareDeleteNodeFromContext) {
        contextRepository.findById(prepareDeleteNodeFromContext.getContext())
                         .ifPresent(c -> {
                             c.getNode(prepareDeleteNodeFromContext.getNode()).ifPresent(ccn -> {
                                 ccn.setPendingDelete(true);
                                 contextRepository.save(c);
                             });
                         });
    }


    @Transactional
    public void deleteAll() {
        contextRepository.deleteAll();
    }
}
