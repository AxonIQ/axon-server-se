package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

public class DefaultTransformationState implements TransformationState {

    private static final long INITIAL_SEQUENCE = -1L;

    private final List<TransformationEntry> stagedEntries;
    private final EventStoreTransformationJpa entity;

    public DefaultTransformationState(@Nonnull EventStoreTransformationJpa entity) {
        this(entity, Collections.emptyList());
    }

    public DefaultTransformationState(@Nonnull EventStoreTransformationJpa entity,
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
        return Optional.ofNullable(entity.getLastEventToken());
    }


    @Override
    public Optional<String> applier() {
        return Optional.ofNullable(entity.getApplier());
    }

    @Override
    public Optional<Instant> appliedAt() {
        return Optional.ofNullable(entity.getDateApplied().toInstant());
    }

    @Override
    public EventStoreTransformationJpa.Status status() {
        return entity.getStatus();
    }

    @Override
    public TransformationState stage(TransformationAction entry) {
        List<TransformationEntry> staged = new ArrayList<>(stagedEntries);
        long sequence = lastSequence().orElse(INITIAL_SEQUENCE) + 1;
        staged.add(new ProtoTransformationEntry(sequence, entry));
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setLastSequence(sequence);
        return new DefaultTransformationState(jpa, staged);
    }

    @Override
    public List<TransformationEntry> staged() {
        return Collections.unmodifiableList(stagedEntries);
    }

    @Override
    public TransformationState applying(String requester) {
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setStatus(EventStoreTransformationJpa.Status.APPLYING);
        jpa.setApplier(requester);
        return new DefaultTransformationState(jpa, stagedEntries);
    }

    @Override
    public TransformationState applied() {
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setStatus(EventStoreTransformationJpa.Status.APPLIED);
        jpa.setDateApplied(new Date());
        return new DefaultTransformationState(jpa, stagedEntries);
    }

    @Override
    public DefaultTransformationState withStatus(EventStoreTransformationJpa.Status status) {
        //todo remove JPA status from interface
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setStatus(status);
        return new DefaultTransformationState(jpa, stagedEntries);
    }

    @Override
    public TransformationState withLastEventToken(long token) {
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa(entity);
        jpa.setLastEventToken(token);
        return new DefaultTransformationState(jpa, stagedEntries);
    }

    @Override
    public String toString() {
        return "JpaTransformationState{" +
                "stagedEntries=" + stagedEntries +
                ", entity=" + entity +
                '}';
    }
}
