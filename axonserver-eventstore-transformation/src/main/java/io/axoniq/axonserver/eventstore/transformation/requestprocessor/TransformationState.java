package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface TransformationState {

    String id();

    int version();

    String description();

    /**
     * Returns the sequence of the last transformation action, {@link Optional#empty()} if the transformation doesn't
     * contain any action yet
     *
     * @return the sequence of the last transformation action
     */
    Optional<Long> lastSequence();

    Optional<Long> lastEventToken();

    Optional<Applied> applied();

    // TODO: 12/30/21 separate this
    EventStoreTransformationJpa.Status status();

    TransformationState stage(TransformationAction entry);

    List<TransformationEntry> staged();

    TransformationState withStatus(EventStoreTransformationJpa.Status status);

    interface Applied {

        String by();

        Instant at();
    }
}
