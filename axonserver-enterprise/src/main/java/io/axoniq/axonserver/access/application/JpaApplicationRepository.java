package io.axoniq.axonserver.access.application;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface JpaApplicationRepository extends JpaRepository<JpaApplication, Long> {

    JpaApplication findFirstByName(String name);

    List<JpaApplication> findAllByTokenPrefix(String prefix);

    List<JpaApplication> findAllByContextsContext(String prefix);

    void deleteAllByContextsContext(String context);
}
