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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Marc Gathier
 */
public class InputStreamEventStore extends SegmentBasedEventStore {
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamEventStore(EventTypeContext context, IndexManager indexManager,
                               EventTransformerFactory eventTransformerFactory,
                               StorageProperties storageProperties) {
        super(context, indexManager, storageProperties);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        segments.add(segment);
        callback.run();
    }

    @Override
    public void initSegments(long lastInitialized) {
        segments.addAll(prepareSegmentStore(lastInitialized));
        if( next != null) next.initSegments(segments.isEmpty() ? lastInitialized : segments.last());

    }

    @Override
    public void close(boolean deleteData) {
        if( deleteData) {
            segments.forEach(this::removeSegment);
        }
    }


        private void removeSegment(long segment) {
            if( segments.remove(segment)) {


                indexManager.remove(segment);
                if( ! FileUtils.delete(storageProperties.dataFile(context, segment)) ||
                        ! FileUtils.delete(storageProperties.index(context, segment)) ||
                        ! FileUtils.delete(storageProperties.bloomFilter(context, segment)) ) {
                    throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, "Failed to rollback " +getType().getEventType() + ", could not remove segment: " + segment);
                }
            }

    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        logger.debug("Get eventsource: {}", segment);
        InputStreamEventSource eventSource = get(segment, false);
        logger.trace("result={}", eventSource);
        if( eventSource == null)
            return Optional.empty();
        return Optional.of(eventSource);
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments;
    }

    @Override
    protected SortedSet<PositionInfo> getPositions(long segment, String aggregateId) {
        return indexManager.getPositions(segment, aggregateId   );
    }

    @Override
    public void deleteAllEventData() {
        throw new UnsupportedOperationException("Development mode deletion is not supported in InputStreamEventStore");
    }

    private InputStreamEventSource get(long segment, boolean force) {
        if( !force && ! segments.contains(segment)) return null;

        return new InputStreamEventSource(storageProperties.dataFile(context, segment), eventTransformerFactory, storageProperties);
    }

    @Override
    protected void recreateIndex(long segment) {
        try (InputStreamEventSource is = get(segment, true);
             EventIterator iterator = createEventIterator( is,segment, segment)) {
            Map<String, SortedSet<PositionInfo>> aggregatePositions = new HashMap<>();
            while (iterator.hasNext()) {
                EventInformation event = iterator.next();
                if (event.isDomainEvent()) {
                    aggregatePositions.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
                                                       k -> new ConcurrentSkipListSet<>())
                                      .add(new PositionInfo(event.getPosition(),
                                                            event.getEvent().getAggregateSequenceNumber()));
                }
            }
            indexManager.createIndex(segment, aggregatePositions);
        }

    }

}
