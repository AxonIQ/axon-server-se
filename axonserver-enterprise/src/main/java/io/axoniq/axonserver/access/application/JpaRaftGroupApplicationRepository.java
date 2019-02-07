package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.enterprise.jpa.JpaRaftGroupApplication;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * Author: marc
 */
public interface JpaRaftGroupApplicationRepository extends JpaRepository<JpaRaftGroupApplication, Long> {

    Optional<JpaRaftGroupApplication> findJpaRaftGroupApplicationByGroupIdAndName(String groupId, String name);
}
