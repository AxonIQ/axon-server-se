package io.axoniq.axonserver.access.application;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface JpaContextUserRepository extends JpaRepository<JpaContextUser, Long> {

    Optional<JpaContextUser> findByContextAndUsername(String context, String username);

    Iterable<JpaContextUser> findAllByContext(String context);
}
