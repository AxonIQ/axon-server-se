package io.axoniq.axonserver.access.pathmapping;

import io.axoniq.axonserver.access.jpa.PathMapping;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JpaRepostory to access {@link PathMapping} entities.
 *
 * @author Marc Gathier
 */
public interface PathMappingRepository extends JpaRepository<PathMapping, String> {
}
