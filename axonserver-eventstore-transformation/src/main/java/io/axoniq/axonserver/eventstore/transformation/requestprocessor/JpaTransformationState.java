package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nonnull;

public class JpaTransformationState implements TransformationState {

    private static final long INITIAL_SEQUENCE = -1L;

    private final List<TransformationEntry> stagedEntries;
    private final EventStoreTransformationJpa entity;

    public JpaTransformationState(@Nonnull EventStoreTransformationJpa entity) {
        this(entity, Collections.emptyList());
    }

    public JpaTransformationState(@Nonnull EventStoreTransformationJpa entity,
                                  @Nonnull List<TransformationEntry> stagedEntries) {
        this.entity = entity;
        this.stagedEntries = new LinkedList<>(stagedEntries);
    }

    @Override
    public String id() {
        return entity.getTransformationId();
    }

    @Override
    public int version() {
        return entity.getVersion();
    }

    @Override
    public String description() {
        return entity.getDescription();
    }

    @Override
    public Optional<Long> lastSequence() {
        return Optional.ofNullable(entity.getLastSequence());
    }

    @Override
    public Optional<Long> lastEventToken() {
        return Optional.empty();
    }

    @Override
    public Optional<Applied> applied() {
        if (entity.getAppliedBy() == null) {
            return Optional.empty();
        }
        return Optional.of(new Applied() {
            @Override
            public String by() {
                return entity.getAppliedBy();
            }

            @Override
            public Instant at() {
                return entity.getDateApplied().toInstant();
            }

            @Override
            public Boolean keepingOldVersion() {
                return entity.isKeepingOldVersions();
            }
        });
    }

    @Override
    public EventStoreTransformationJpa.Status status() {
        return entity.getStatus();
    }

    @Override
    public TransformationState stage(TransformationAction entry) {
        List<TransformationEntry> staged = new ArrayList<>(stagedEntries);
        staged.add((new ProtoTransformationEntry(lastSequence().orElse(INITIAL_SEQUENCE) + 1, entry)));
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setLastSequence(entity.getLastSequence() + 1);
        return new JpaTransformationState(jpa, staged);
    }

    @Override
    public List<TransformationEntry> staged() {
        return Collections.unmodifiableList(stagedEntries);
    }

    @Override
    public JpaTransformationState withStatus(EventStoreTransformationJpa.Status status) {
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setStatus(status);
        return new JpaTransformationState(jpa, stagedEntries);
    }

    @Override
    public String toString() {
        return "JpaTransformationState{" +
                "stagedEntries=" + stagedEntries +
                ", entity=" + entity +
                '}';
    }
}
