package io.axoniq.axonserver.cluster.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface ReplicationGroupRepository extends JpaRepository<ReplicationGroup, String> {

    Optional<ReplicationGroup> findByName(String name);
}
