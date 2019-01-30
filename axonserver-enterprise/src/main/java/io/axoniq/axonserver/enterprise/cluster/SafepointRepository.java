package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.jpa.Safepoint;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface SafepointRepository extends JpaRepository<Safepoint, Safepoint.SafepointKey> {
}
