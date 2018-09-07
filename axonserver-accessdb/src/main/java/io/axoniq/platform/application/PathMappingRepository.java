package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.PathMapping;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by marc on 7/13/2017.
 */
public interface PathMappingRepository extends JpaRepository<PathMapping, String> {
}
