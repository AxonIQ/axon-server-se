package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.enterprise.jpa.JpaRaftGroupApplication;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.springframework.stereotype.Component;

import java.util.HashSet;

/**
 * Author: marc
 */
@Component
public class JpaRaftGroupApplicationController {
    private final JpaRaftGroupApplicationRepository repository;

    public JpaRaftGroupApplicationController(
            JpaRaftGroupApplicationRepository repository) {
        this.repository = repository;
    }

    public void mergeApplication(ContextApplication application) {
        JpaRaftGroupApplication jpaRaftGroupApplication = repository.findJpaRaftGroupApplicationByGroupIdAndName(application.getContext(), application.getName())
                                                                    .orElse(new JpaRaftGroupApplication(application.getContext(), application.getName()));
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
}
