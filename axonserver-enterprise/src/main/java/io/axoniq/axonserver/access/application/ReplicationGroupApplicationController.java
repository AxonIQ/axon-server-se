package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Component
public class ReplicationGroupApplicationController {

    private final ReplicationGroupApplicationRepository repository;

    public ReplicationGroupApplicationController(
            ReplicationGroupApplicationRepository repository) {
        this.repository = repository;
    }

    public void mergeApplication(ContextApplication application) {
        ReplicationGroupApplication jpaRaftGroupApplication = repository.findJpaContextApplicationByContextAndName(
                application.getContext(),
                application.getName())
                                                                        .orElse(new ReplicationGroupApplication(
                                                                                application.getContext(),
                                                                                application.getName()));
        if (application.getRolesCount() == 0) {
            if (jpaRaftGroupApplication.getId() != null) {
                repository.delete(jpaRaftGroupApplication);
            }
            return;
        }
        jpaRaftGroupApplication.setHashedToken(application.getHashedToken());
        jpaRaftGroupApplication.setTokenPrefix(application.getTokenPrefix());
        jpaRaftGroupApplication.setRoles(new HashSet<>(application.getRolesList()));

        repository.save(jpaRaftGroupApplication);
    }

    public List<ContextApplication> getApplicationsForContexts(List<String> contexts) {
        return repository.findAllByContextIn(contexts).stream().map(this::toContextApplication).collect(Collectors
                                                                                                                .toList());
    }

    private ContextApplication toContextApplication(ReplicationGroupApplication jpaContextApplication) {
        return ContextApplication.newBuilder()
                                 .setName(jpaContextApplication.getName())
                                 .setContext(jpaContextApplication.getContext())
                                 .setHashedToken(jpaContextApplication.getHashedToken())
                                 .setTokenPrefix(jpaContextApplication.getTokenPrefix())
                                 .addAllRoles(jpaContextApplication.getRoles())
                                 .build();
    }

    public void deleteByContext(String context) {
        repository.findAllByContext(context).forEach(repository::delete);
    }

    @EventListener
    @Transactional
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        deleteByContext(contextDeleted.context());
    }

    public void deleteApplication(String context, String name) {
        repository.findJpaContextApplicationByContextAndName(context, name)
                  .ifPresent(repository::delete);
    }
}
