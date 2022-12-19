package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface EventStoreStateRepository extends JpaRepository<EventStoreState, String> {


    Iterable<EventStoreState> findByState(State state);

    Optional<EventStoreState> findOptionalByInProgressOperationId(String inProgressOperationId);
}
