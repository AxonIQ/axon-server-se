package io.axoniq.axonserver.cluster.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 * @since 4.1
 */

public interface JpaRaftStateRepository extends JpaRepository<JpaRaftState, String> {

}
