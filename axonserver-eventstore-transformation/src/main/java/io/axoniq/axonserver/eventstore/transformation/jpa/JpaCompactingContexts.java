package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.compact.CompactingContexts;

import java.util.Iterator;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreStateJpa.State.COMPACTING;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class JpaCompactingContexts implements CompactingContexts {

    private final EventStoreStateRepository repo;

    public JpaCompactingContexts(EventStoreStateRepository repo) {
        this.repo = repo;
    }

    @Nonnull
    @Override
    public Iterator<CompactingContext> iterator() {
        return new Iterator<CompactingContext>() {
            final Iterator<EventStoreStateJpa> iterator = repo.findByState(COMPACTING).iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public CompactingContext next() {
                return new JpaCompactingContext(iterator.next());
            }
        };
    }


    private static class JpaCompactingContext implements CompactingContext {

        private final EventStoreStateJpa eventStoreStateJpa;

        private JpaCompactingContext(EventStoreStateJpa eventStoreStateJpa) {
            this.eventStoreStateJpa = eventStoreStateJpa;
        }

        @Override
        public String compactionId() {
            return eventStoreStateJpa.inProgressOperationId();
        }

        @Override
        public String context() {
            return eventStoreStateJpa.context();
        }
    }
}
