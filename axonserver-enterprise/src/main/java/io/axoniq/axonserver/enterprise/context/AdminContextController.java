package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminContextRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages updates in context configuration as they are stored for the ADMIN nodes.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Controller
public class AdminContextController {

    private final Logger logger = LoggerFactory.getLogger(AdminContextController.class);
    private final AdminContextRepository contextRepository;

    /**
     * Constructor for the context controller.
     *
     * @param contextRepository for access to stored context data
     */
    public AdminContextController(
            AdminContextRepository contextRepository) {
        this.contextRepository = contextRepository;
    }

    /**
     * Get a stream of all contexts.
     *
     * @return stream of all contexts.
     */
    public Stream<AdminContext> getContexts() {
        return contextRepository.findAll()
                                .stream();
    }

    /**
     * Returns a context based on its name. Returns null if context not found.
     *
     * @param contextName the name of the context
     * @return the context or null
     */
    public AdminContext getContext(String contextName) {
        return contextRepository.findById(contextName).orElse(null);
    }

    @Transactional
    public void deleteContext(String context) {
        contextRepository.deleteById(context);
    }

    public Set<String> findConnectableNodes(String context) {
        return contextRepository.findById(context)
                                .map(c -> c.getReplicationGroup()
                                           .getMembers()
                                           .stream()
                                           .filter(m -> RoleUtils.allowsClientConnect(m.getRole()))
                                           .map(m -> m.getClusterNode().getName())
                                           .collect(Collectors.toSet()))
                                .orElse(Collections.emptySet());
    }
}
