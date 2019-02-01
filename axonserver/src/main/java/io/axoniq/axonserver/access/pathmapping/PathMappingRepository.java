package io.axoniq.axonserver.access.pathmapping;

import io.axoniq.axonserver.access.jpa.PathMapping;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface PathMappingRepository extends JpaRepository<PathMapping, String> {
}
