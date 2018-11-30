package io.axoniq.axonserver.cluster.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */

public interface JpaRaftStateRepository extends JpaRepository<JpaRaftState, String> {

}
