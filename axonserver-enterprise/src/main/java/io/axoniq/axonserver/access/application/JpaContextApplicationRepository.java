package io.axoniq.axonserver.access.application;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public interface JpaContextApplicationRepository extends JpaRepository<JpaContextApplication, Long> {

    Optional<JpaContextApplication> findJpaContextApplicationByContextAndName(String context, String name);

    List<JpaContextApplication> findAllByTokenPrefix(String tokenPrefix);

    List<JpaContextApplication> findAllByContext(String context);

    /**
     * Retries apps based on context and first characters in token.
     *
     * @param tokenPrefix the first characters of the token
     * @param context     the context to retrieve applications for
     * @return a list of applications
     */
    List<JpaContextApplication> findAllByTokenPrefixAndContext(String tokenPrefix, String context);
}
