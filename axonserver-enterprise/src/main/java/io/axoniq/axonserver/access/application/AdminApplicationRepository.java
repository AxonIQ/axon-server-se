package io.axoniq.axonserver.access.application;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface AdminApplicationRepository extends JpaRepository<AdminApplication, Long> {

    AdminApplication findFirstByName(String name);

    List<AdminApplication> findAllByTokenPrefix(String prefix);

    List<AdminApplication> findAllByContextsContext(String context);
}
