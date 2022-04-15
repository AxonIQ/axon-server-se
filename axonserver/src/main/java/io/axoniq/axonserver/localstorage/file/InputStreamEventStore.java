/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.MeterFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;

/**
 * Manages the completed segments for the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class InputStreamEventStore extends SegmentBasedEventStore implements ReadOnlySegmentsHandler {

    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamEventStore(EventTypeContext context, IndexManager indexManager,
                                 EventTransformerFactory eventTransformerFactory,
                                 Supplier<StorageProperties> storagePropertiesSupplier, MeterFactory meterFactory) {
        super(context, indexManager, storagePropertiesSupplier, meterFactory);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        segments.add(segment);
        callback.run();
    }

    @Override
    protected boolean containsSegment(long segment) {
        return segments.contains(segment);
    }

    @Override
    public void initSegments(long lastInitialized) {
        segments.addAll(prepareSegmentStore(lastInitialized));
        if (next != null) {
            next.initSegments(segments.isEmpty() ? lastInitialized : segments.last());
        }
    }

    @Override
    public void close(boolean deleteData) {
        if (deleteData) {
            segments.forEach(this::removeSegment);
        }
    }


    private void removeSegment(long segment) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        if (segments.remove(segment) && (!FileUtils.delete(storageProperties.dataFile(context, segment)) ||
                !indexManager.remove(segment))) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Failed to rollback " + getType().getEventType()
                                                         + ", could not remove segment: " + segment);
        }
    }

    @Override
    public Optional<EventSource> getEventSource(long segment) {
        logger.debug("Get eventsource: {}", segment);
        InputStreamEventSource eventSource = get(segment, false);
        logger.trace("result={}", eventSource);
        if (eventSource == null) {
            return Optional.empty();
        }
        return Optional.of(eventSource);
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments;
    }

    private InputStreamEventSource get(long segment, boolean force) {
        if (!force && !segments.contains(segment)) {
            return null;
        }

        fileOpenMeter.increment();
        return new InputStreamEventSource(storagePropertiesSupplier.get().dataFile(context, segment),
                                          eventTransformerFactory);
    }

    @Override
    protected void recreateIndex(long segment) {
        try (InputStreamEventSource is = get(segment, true);
             EventIterator iterator = createEventIterator(is, segment, segment)) {
            recreateIndexFromIterator(segment, iterator);
        }
    }
}
