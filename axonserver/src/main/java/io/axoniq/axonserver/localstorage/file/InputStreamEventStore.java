/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.MeterFactory;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

/**
 * Manages the completed segments for the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class InputStreamEventStore extends SegmentBasedEventStore implements StorageTier {

    private final NavigableMap<Long, Integer> segments = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamEventStore(EventTypeContext context, IndexManager indexManager,
                                 EventTransformerFactory eventTransformerFactory,
                                 Supplier<StorageProperties> storageProperties,
                                 MeterFactory meterFactory, String storagePath) {
        super(context, indexManager, storageProperties, meterFactory, storagePath);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    //add FileVersion to the Segment
    @Override
    public void handover(Segment segment, Runnable callback) {
        segments.put(segment.id().segment(), segment.id().segmentVersion());
        callback.run();
    }

    @Override
    protected boolean containsSegment(long segment) {
        return segments.containsKey(segment);
    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        Integer segmentVersion = segments.get(segment);
        if (segmentVersion != null) {
            return getEventSource(new FileVersion(segment, segmentVersion));
        }
        return Optional.empty();
    }

    @Override
    public void initSegments(long lastInitialized) {
        segments.putAll(prepareSegmentStore(lastInitialized));
        if (next.get() != null) {
            next.get().initSegments(segments.isEmpty() ? lastInitialized : segments.navigableKeySet().last());
        }
    }

    @Override
    public void close(boolean deleteData) {
        if (deleteData) {
            segments.forEach(this::removeAllSegmentVersions);
        }
    }

    private void removeAllSegmentVersions(Long segment, int currentVersion) {
        for (int i = 0; i <= currentVersion; i++) {
            removeSegment(segment, i);
        }
    }

    //todo fix me
    @Override
    public boolean removeSegment(long segment, int segmentVersion) {
        return indexManager.remove(new FileVersion(segment, segmentVersion)) &&
                FileUtils.delete(dataFile(new FileVersion(segment, segmentVersion)));
    }

    //todo check if we should keep it
//        private void removeSegment(long segment) {
//            if (segments.remove(segment) && (!FileUtils.delete(dataFile(segment)) ||
//                    !indexManager.remove(segment))) {
//                throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
//                        "Failed to rollback " + getType().getEventType()
//                                + ", could not remove segment: " + segment);
//            }

    @Override
    public Integer currentSegmentVersion(Long segment) {
        return segments.get(segment);
    }

    @Override
    public void activateSegmentVersion(long segment, int segmentVersion) {
        segments.put(segment, segmentVersion);
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
    protected SortedSet<Long> doGetSegments() {
        return segments.navigableKeySet();
    }

    private InputStreamEventSource get(FileVersion segment, boolean force) {
        if (!force && !segments.containsKey(segment.segment())) {
            return null;
        }

        fileOpenMeter.increment();
        return new InputStreamEventSource(dataFile(segment),
                                          segment.segment(),
                                          segment.segmentVersion(),
                                          eventTransformerFactory);
    }

    @Override
    protected void recreateIndex(FileVersion segment) {
        try (InputStreamEventSource is = get(segment, true);
             EventIterator iterator = createEventIterator(is, segment.segment())) {
            recreateIndexFromIterator(segment, iterator);
        }
    }
}
