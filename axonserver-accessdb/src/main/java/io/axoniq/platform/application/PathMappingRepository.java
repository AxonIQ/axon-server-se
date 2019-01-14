package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.PathMapping;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface PathMappingRepository extends JpaRepository<PathMapping, String> {
}
