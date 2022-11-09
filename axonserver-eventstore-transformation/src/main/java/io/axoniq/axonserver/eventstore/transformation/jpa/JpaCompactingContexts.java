package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.compact.CompactingContexts;

import java.util.Iterator;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State.COMPACTING;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class JpaCompactingContexts implements CompactingContexts {

    private final JpaEventStoreStateRepo repo;

    public JpaCompactingContexts(JpaEventStoreStateRepo repo) {
        this.repo = repo;
    }

    @Nonnull
    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            final Iterator<EventStoreState> iterator = repo.findByState(COMPACTING).iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().getContext();
            }
        };
    }
}
