package io.axoniq.axonhub.cluster;

import io.axoniq.axonhub.cluster.jpa.Safepoint;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface SafepointRepository extends JpaRepository<Safepoint, Safepoint.SafepointKey> {
}
