package io.axoniq.axonserver.access.modelversion;

import io.axoniq.axonserver.access.jpa.ApplicationModelVersion;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ApplicationModelVersionRepository extends JpaRepository<ApplicationModelVersion, String> {
}
