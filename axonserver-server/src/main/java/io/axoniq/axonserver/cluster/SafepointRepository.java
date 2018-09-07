package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.jpa.Safepoint;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface SafepointRepository extends JpaRepository<Safepoint, Safepoint.SafepointKey> {
}
