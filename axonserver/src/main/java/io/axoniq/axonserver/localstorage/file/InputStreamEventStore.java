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
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Manages the completed segments for the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class InputStreamEventStore extends SegmentBasedEventStore implements ReadOnlySegmentsHandler {

    private final NavigableMap<Long, Integer> segments = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamEventStore(EventTypeContext context, IndexManager indexManager,
                                 EventTransformerFactory eventTransformerFactory,
                                 StorageProperties storageProperties, MeterFactory meterFactory) {
        super(context, indexManager, storageProperties, meterFactory);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    public void handover(FileVersion segment, Runnable callback) {
        segments.put(segment.segment(), segment.version());
        callback.run();
    }

    @Override
    protected boolean containsSegment(long segment) {
        return segments.containsKey(segment);
    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        Integer version = segments.get(segment);
        if (version != null) {
            return getEventSource(new FileVersion(segment, version));
        }
        return Optional.empty();
    }

    @Override
    public void initSegments(long lastInitialized) {
        segments.putAll(prepareSegmentStore(lastInitialized));
        if (next != null) {
            next.initSegments(segments.isEmpty() ? lastInitialized : segments.navigableKeySet().last());
        }
    }

    @Override
    public void close(boolean deleteData) {
        if (deleteData) {
            segments.forEach((segment, version) -> removeSegment(segment));
        }
    }

    @Override
    protected Integer currentSegmentVersion(Long segment) {
        return segments.get(segment);
    }

    @Override
    protected void segmentActiveVersion(long segment, int version) {
        segments.put(segment, version);
    }

    private void removeSegment(long segment) {
        Integer version = segments.remove(segment);
        if (version != null && (!FileUtils.delete(storageProperties.dataFile(context, segment)) ||
                !indexManager.remove(segment))) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Failed to rollback " + getType().getEventType()
                                                         + ", could not remove segment: " + segment);
        }
    }

    @Override
    protected void removeSegment(long segment, int currentVersion) {
        if ( !FileUtils.delete(storageProperties.dataFile(context, new FileVersion(segment, currentVersion))) ||
                !indexManager.remove(new FileVersion(segment, currentVersion))) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Failed to rollback " + getType().getEventType()
                                                         + ", could not remove segment: " + segment);
        }
    }

    @Override
    public Optional<EventSource> getEventSource(FileVersion segment) {
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
        return segments.navigableKeySet();
    }

    private InputStreamEventSource get(FileVersion segment, boolean force) {
        if (!force && !segments.containsKey(segment.segment())) {
            return null;
        }

        fileOpenMeter.increment();
        return new InputStreamEventSource(storageProperties.dataFile(context, segment),
                                          eventTransformerFactory);
    }

    @Override
    protected void recreateIndex(FileVersion segment) {
        try (InputStreamEventSource is = get(segment, true);
             EventIterator iterator = createEventIterator(is, segment.segment(), segment.segment())) {
            recreateIndexFromIterator(segment, iterator);
        }
    }
}
