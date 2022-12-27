package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreStateJpa.State;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface EventStoreStateRepository extends JpaRepository<EventStoreStateJpa, String> {


    Iterable<EventStoreStateJpa> findByState(State state);

    Optional<EventStoreStateJpa> findOptionalByInProgressOperationId(String inProgressOperationId);
}
