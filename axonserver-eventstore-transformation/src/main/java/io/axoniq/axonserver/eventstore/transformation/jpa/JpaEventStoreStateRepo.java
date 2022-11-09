package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface JpaEventStoreStateRepo extends JpaRepository<EventStoreState, String> {


    List<EventStoreState> findByState(State state);
}
