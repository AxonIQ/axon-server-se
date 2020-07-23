package io.axoniq.axonserver.enterprise.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface ReplicationGroupContextRepository extends JpaRepository<ReplicationGroupContext, String> {

    List<ReplicationGroupContext> findByReplicationGroupName(String replicationGroupName);
}
