package io.axoniq.axonserver.eventstore.transformation.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
@Entity
@Table(name="et_event_store_state")
public class EventStoreStateJpa {

    @Id
    private String context;
    @Enumerated(EnumType.STRING)
    private State state;

    private String inProgressOperationId;

    public String context() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public State state() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String inProgressOperationId() {
        return inProgressOperationId;
    }

    public void setInProgressOperationId(String inProgressOperationId) {
        this.inProgressOperationId = inProgressOperationId;
    }

    public enum State {
        IDLE,
        TRANSFORMING,
        COMPACTING
    }
}
