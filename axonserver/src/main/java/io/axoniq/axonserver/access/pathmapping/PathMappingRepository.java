package io.axoniq.axonserver.access.pathmapping;

import io.axoniq.axonserver.access.jpa.PathMapping;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by marc on 7/13/2017.
 */
public interface PathMappingRepository extends JpaRepository<PathMapping, String> {
}
