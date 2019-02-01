package io.axoniq.axonserver.access.modelversion;

import io.axoniq.axonserver.access.jpa.ApplicationModelVersion;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface ApplicationModelVersionRepository extends JpaRepository<ApplicationModelVersion, String> {
}
