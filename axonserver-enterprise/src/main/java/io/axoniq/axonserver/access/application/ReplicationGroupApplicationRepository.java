package io.axoniq.axonserver.access.application;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public interface ReplicationGroupApplicationRepository extends JpaRepository<ReplicationGroupApplication, Long> {

    Optional<ReplicationGroupApplication> findJpaContextApplicationByContextAndName(String context, String name);

    List<ReplicationGroupApplication> findAllByTokenPrefix(String tokenPrefix);

    List<ReplicationGroupApplication> findAllByContext(String context);

    List<ReplicationGroupApplication> findAllByContextIn(List<String> context);

    /**
     * Retries apps based on context and first characters in token.
     *
     * @param tokenPrefix the first characters of the token
     * @param context     the context to retrieve applications for
     * @return a list of applications
     */
    List<ReplicationGroupApplication> findAllByTokenPrefixAndContext(String tokenPrefix, String context);
}
