package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.ApplicationModelVersion;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ApplicationModelVersionRepository extends JpaRepository<ApplicationModelVersion, String> {
}
