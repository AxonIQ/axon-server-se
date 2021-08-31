package io.axoniq.axonserver.requestprocessor.eventstore;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationRepository extends JpaRepository<EventStoreTransformationJpa, String> {
}
