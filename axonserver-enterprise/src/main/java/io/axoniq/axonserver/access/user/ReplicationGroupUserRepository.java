package io.axoniq.axonserver.access.user;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface ReplicationGroupUserRepository extends JpaRepository<ReplicationGroupUser, Long> {

    Optional<ReplicationGroupUser> findByContextAndUsername(String context, String username);

    Iterable<ReplicationGroupUser> findAllByContext(String context);

    Iterable<ReplicationGroupUser> findAllByContextIn(List<String> contextNames);
}
