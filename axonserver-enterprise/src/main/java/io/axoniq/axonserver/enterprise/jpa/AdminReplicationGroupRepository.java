package io.axoniq.axonserver.enterprise.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface AdminReplicationGroupRepository extends JpaRepository<AdminReplicationGroup, String> {

    Optional<AdminReplicationGroup> findByName(String name);
}
