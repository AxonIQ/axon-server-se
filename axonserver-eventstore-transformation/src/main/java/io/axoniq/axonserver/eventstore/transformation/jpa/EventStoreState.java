package io.axoniq.axonserver.eventstore.transformation.jpa;

import java.time.Instant;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
@Entity
public class EventStoreState {

    @Id
    private String context;
    @Enumerated(EnumType.STRING)
    private State state;
    private Instant lastUpdate;

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Instant getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Instant lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public enum State {
        IDLE,
        TRANSFORMING,
        COMPACTING
    }
}
