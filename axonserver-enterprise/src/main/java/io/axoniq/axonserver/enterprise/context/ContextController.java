package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Controller;

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

    public ContextController(
            EntityManager entityManager,
            ClusterController clusterController,
            ApplicationEventPublisher eventPublisher) {
        this.entityManager = entityManager;
        this.clusterController = clusterController;
        this.eventPublisher = eventPublisher;
    }

    public Stream<Context> getContexts() {
        return entityManager.createQuery("select c from Context c", Context.class)
                            .getResultList()
                            .stream();
    }

    public Context getContext(String contextName){
        return entityManager.find(Context.class, contextName);
    }
}
