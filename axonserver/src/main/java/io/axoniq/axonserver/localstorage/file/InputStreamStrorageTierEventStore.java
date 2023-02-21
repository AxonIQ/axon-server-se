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

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Manages the completed segments for the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class InputStreamStrorageTierEventStore extends AbstractFileStorageTier {

    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamStrorageTierEventStore(EventTypeContext context, IndexManager indexManager,
                                             EventTransformerFactory eventTransformerFactory,
                                             Supplier<StorageProperties> storageProperties,
                                             MeterFactory meterFactory, String storagePath) {
        super(context, indexManager, storageProperties, meterFactory, storagePath);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    public void handover(Segment segment, Runnable callback) {
        segments.put(segment.id().segment(), segment.id().segmentVersion());
        callback.run();
    }

    @Override
    protected Optional<EventSource> localEventSource(long segment) {
        Integer segmentVersion = segments.get(segment);
        if (segmentVersion != null) {
            return localEventSource(new FileVersion(segment, segmentVersion));
        }
        return Optional.empty();
    }

    @Override
    public void initSegments(long lastInitialized) {
        prepareSegmentStore(lastInitialized);
        applyOnNext(n -> n.initSegments(segments.isEmpty() ? lastInitialized : segments.navigableKeySet().last()));
    }

    @Override
    public void close(boolean deleteData) {
        if (deleteData) {
            segments.forEach((key, value) -> {
                Set<Integer> versions = versions(key, value);
                versions.forEach(v -> removeSegment(key, v));
            });
            segments.clear();
        }
    }


    @Override
    public boolean removeSegment(long segment, int segmentVersion) {
        return indexManager.remove(new FileVersion(segment, segmentVersion)) &&
                FileUtils.delete(dataFile(new FileVersion(segment, segmentVersion)));
    }



    @Override
    protected Optional<EventSource> localEventSource(FileVersion segment) {
        logger.debug("Get eventsource: {}", segment);
        InputStreamEventSource eventSource = get(segment, false);
        logger.trace("result={}", eventSource);
        if (eventSource == null) {
            return Optional.empty();
        }
        return Optional.of(eventSource);
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
}
