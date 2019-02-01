package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.Application;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface ApplicationRepository extends JpaRepository<Application, Long> {

    Application findFirstByName(String name);

    List<Application> findAllByTokenPrefix(String prefix);

    List<Application> findAllByContextsContext(String prefix);

    void deleteAllByContextsContext(String context);
}
