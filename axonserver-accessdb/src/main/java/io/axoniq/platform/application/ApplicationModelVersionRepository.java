package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.ApplicationModelVersion;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface ApplicationModelVersionRepository extends JpaRepository<ApplicationModelVersion, String> {
}
