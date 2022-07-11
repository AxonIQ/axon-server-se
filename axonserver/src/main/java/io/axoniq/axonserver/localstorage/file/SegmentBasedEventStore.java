/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.axoniq.axonserver.localstorage.file.FileUtils.name;
import static io.axoniq.axonserver.localstorage.file.UpgradeUtils.fixInvalidFileName;


/**
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class SegmentBasedEventStore implements SegmentGroup {

    protected static final Logger logger = LoggerFactory.getLogger(SegmentBasedEventStore.class);
    protected static final int VERSION_BYTES = 1;
    protected static final int FILE_OPTIONS_BYTES = 4;
    protected final String context;
    protected final IndexManager indexManager;
    protected final Supplier<StorageProperties> storagePropertiesSupplier;
    protected final EventTypeContext type;
    protected final SegmentGroup next;
    protected final Counter fileOpenMeter;

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager,
                                  Supplier<StorageProperties> storagePropertiesSupplier, MeterFactory meterFactory) {
        this(eventTypeContext, indexManager, storagePropertiesSupplier, null, meterFactory);
    }

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager,
                                  Supplier<StorageProperties> storagePropertiesSupplier,
                                  SegmentGroup nextSegmentsHandler,
                                  MeterFactory meterFactory) {
        this.type = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storagePropertiesSupplier = storagePropertiesSupplier;
        this.next = nextSegmentsHandler;
        Tags tags = Tags.of(MeterFactory.CONTEXT, context, "type", eventTypeContext.getEventType().name());
        this.fileOpenMeter = meterFactory.counter(BaseMetricName.AXON_SEGMENT_OPEN, tags);
    }


    @Override
    public long getFirstCompletedSegment() {
        if (getSegments().isEmpty()) {
            return -1;
        }
        return getSegments().first();
    }

    @Override
    public long getFirstToken() {
        if (next != null && !next.getSegments().isEmpty()) {
            return next.getFirstToken();
        }
        if (getSegments().isEmpty()) {
            return -1;
        }
        return getSegments().last();
    }

    @Override
    public long getTokenAt(long instant) {
        for (long segment : getSegments()) {
            Optional<EventSource> eventSource = getEventSource(segment);
            Long found = eventSource.map(es -> {
                try (EventIterator iterator = es.createEventIterator(segment, segment)) {
                    return iterator.getTokenAt(instant);
                }
            }).orElse(null);

            if (found != null) {
                return found;
            }
        }

        if (next != null) {
            return next.getTokenAt(instant);
        }
        return getSegments().isEmpty() ? -1 : getFirstToken();
    }


    public EventTypeContext getType() {
        return type;
    }
    public long getSegmentFor(long token) {
        return getSegments().stream()
                            .filter(segment -> segment <= token)
                            .findFirst()
                            .orElse(next == null ? -1 : next.getSegmentFor(token));
    }


    protected SortedSet<Long> prepareSegmentStore(long lastInitialized) {
        SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File events = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(events);
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());
        Arrays.stream(eventFiles)
              .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
              .filter(segment -> segment < lastInitialized)
              .forEach(segments::add);

        segments.forEach(this::renameFileIfNecessary);
        long firstValidIndex = segments.stream().filter(indexManager::validIndex).findFirst().orElse(-1L);
        logger.debug("First valid index: {}", firstValidIndex);
        SortedSet<Long> recreate = new TreeSet<>();
        recreate.addAll(segments.headSet(firstValidIndex));
        recreate.forEach(this::recreateIndex);
        return segments;
    }

    protected abstract void recreateIndex(long segment);

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        Stream<String> filenames = Stream.concat(getSegments().stream()
                                                              .filter(s -> s > lastSegmentBackedUp)
                                                              .map(s -> name(storageProperties.dataFile(context, s))),
                                                 indexManager.getBackupFilenames(lastSegmentBackedUp));

        if (next == null) {
            return filenames;
        }
        return Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp));
    }

    protected void renameFileIfNecessary(long segment) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        fixInvalidFileName(segment, storageProperties, context);
    }


    protected void recreateIndexFromIterator(long segment, EventIterator iterator) {
        Map<String, List<IndexEntry>> loadedEntries = new HashMap<>();
        while (iterator.hasNext()) {
            EventInformation event = iterator.next();
            if (event.isDomainEvent()) {
                IndexEntry indexEntry = new IndexEntry(
                        event.getEvent().getAggregateSequenceNumber(),
                        event.getPosition(),
                        event.getToken());
                loadedEntries.computeIfAbsent(event.getEvent().getAggregateIdentifier(), id -> new LinkedList<>())
                             .add(indexEntry);
            }
        }
        indexManager.addToActiveSegment(segment, loadedEntries);
        indexManager.complete(segment);
    }

    public Stream<Long> getAllSegments() {
        if (next == null) {
            return getSegments().stream();
        }
        return Stream.concat(getSegments().stream(), next.getAllSegments()).distinct();
    }


    /**
     * @param segment gets an EventSource for the segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    public abstract Optional<EventSource> getEventSource(long segment);

    //Retrieves event source from first available layer that is responsible for given segment
    public Optional<EventSource> eventSource(long segment) {
        Optional<EventSource> eventSource = getEventSource(segment);
        if (eventSource.isPresent()) {
            return eventSource;
        }
        if (next == null) {
            return Optional.empty();
        }
        return next.eventSource(segment);
    }




}
