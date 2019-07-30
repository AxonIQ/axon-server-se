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
public class JpaContextApplicationController {
    private final JpaContextApplicationRepository repository;

    public JpaContextApplicationController(
            JpaContextApplicationRepository repository) {
        this.repository = repository;
    }

    public void mergeApplication(ContextApplication application) {
        JpaContextApplication jpaRaftGroupApplication = repository.findJpaContextApplicationByContextAndName(application.getContext(), application.getName())
                                                                  .orElse(new JpaContextApplication(application.getContext(), application.getName()));
        if( application.getRolesCount() == 0) {
            if( jpaRaftGroupApplication.getId() != null) {
                repository.delete(jpaRaftGroupApplication);
            }
            return;
        }
        jpaRaftGroupApplication.setHashedToken(application.getHashedToken());
        jpaRaftGroupApplication.setTokenPrefix(application.getTokenPrefix());
        jpaRaftGroupApplication.setRoles(new HashSet<>(application.getRolesList()));

        repository.save(jpaRaftGroupApplication);
    }

    public List<ContextApplication> getApplicationsForContext(String context) {
        return repository.findAllByContext(context).stream().map(this::toContextApplication).collect(Collectors.toList());
    }

    private ContextApplication toContextApplication(JpaContextApplication jpaContextApplication) {
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
        deleteByContext(contextDeleted.getContext());
    }

    public void deleteApplication(String context, String name) {
        repository.findJpaContextApplicationByContextAndName(context, name)
                  .ifPresent(repository::delete);
    }
}
