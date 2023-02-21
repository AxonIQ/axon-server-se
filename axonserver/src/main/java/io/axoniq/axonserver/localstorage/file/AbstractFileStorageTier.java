/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class AbstractFileStorageTier implements StorageTier {

    public static final byte TRANSACTION_VERSION = 2;
    protected static final Logger logger = LoggerFactory.getLogger(AbstractFileStorageTier.class);
    protected static final int VERSION_BYTES = 1;
    protected static final int FILE_OPTIONS_BYTES = 4;
    protected static final int TX_CHECKSUM_BYTES = 4;
    protected static final byte EVENT_FORMAT_VERSION = 2;
    private static final int TRANSACTION_LENGTH_BYTES = 4;
    private static final int NUMBER_OF_EVENTS_BYTES = 2;
    protected static final int HEADER_BYTES = TRANSACTION_LENGTH_BYTES + VERSION_BYTES + NUMBER_OF_EVENTS_BYTES;
    protected static final int FILE_HEADER_SIZE = VERSION_BYTES + FILE_OPTIONS_BYTES;
    protected static final int FILE_FOOTER_SIZE = 4;
    protected static final int MAX_TRANSACTION_SIZE = Integer.MAX_VALUE - FILE_HEADER_SIZE - FILE_FOOTER_SIZE;
    protected final String context;
    protected final IndexManager indexManager;
    protected final Supplier<StorageProperties> storagePropertiesSupplier;
    protected final EventTypeContext eventTypeContext;
    protected final NavigableMap<Long, Integer> segments = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    protected final Supplier<StorageTier> nextSegmentsHandler;
    protected final Counter fileOpenMeter;

    protected final String storagePath;
    private final AtomicBoolean initialized = new AtomicBoolean(true);

    protected AbstractFileStorageTier(EventTypeContext eventTypeContext, IndexManager indexManager,
                                      Supplier<StorageProperties> storagePropertiesSupplier, MeterFactory meterFactory,
                                      String storagePath) {
        this(eventTypeContext, indexManager, storagePropertiesSupplier, () -> null, meterFactory, storagePath);
    }

    protected AbstractFileStorageTier(EventTypeContext eventTypeContext, IndexManager indexManager,
                                      Supplier<StorageProperties> storagePropertiesSupplier,
                                      Supplier<StorageTier> nextSegmentsHandler,
                                      MeterFactory meterFactory,
                                      String storagePath) {
        this.eventTypeContext = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storagePropertiesSupplier = storagePropertiesSupplier;
        this.nextSegmentsHandler = nextSegmentsHandler;
        Tags tags = Tags.of(MeterFactory.CONTEXT, context, "type", eventTypeContext.getEventType().name());
        this.fileOpenMeter = meterFactory.counter(BaseMetricName.AXON_SEGMENT_OPEN, tags);
        this.storagePath = storagePath;
    }

    protected Stream<AggregateSequence> latestSequenceNumbers(FileVersion segment) {
        return indexManager.latestSequenceNumbers(segment).map(indexEntries -> last(segment, indexEntries));
    }

    private AggregateSequence last(FileVersion segment, AggregateIndexEntries indexEntries) {
        if (eventTypeContext.isEvent() || indexEntries.entries().size() == 1) {
            return new AggregateSequence(indexEntries.aggregateId(), indexEntries.entries().lastSequenceNumber());
        }

        return readSerializedEvent(0, Long.MAX_VALUE, new SegmentIndexEntries(segment, indexEntries.entries()))
                .map(event -> new AggregateSequence(indexEntries.aggregateId(), event.getAggregateSequenceNumber()))
                .orElseThrow(() -> new RuntimeException("Failed to read snapshot"));
    }


    public Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber,
                                                         SegmentIndexEntries lastEventPosition) {
        return localEventSource(lastEventPosition.fileVersion())
                .map(e -> {
                    try (e) {
                        return e.readLastInRange(minSequenceNumber,
                                                 maxSequenceNumber,
                                                 lastEventPosition.indexEntries().positions());
                    }
                });
    }

    public Integer currentSegmentVersion(Long segment) {
        Integer version = segments.get(segment);
        if (version != null) {
            return version;
        }

        return invokeOnNext(n -> n.currentSegmentVersion(segment), 0);
    }

    public void activateSegmentVersion(long segment, int segmentVersion) {
        Integer version = segments.get(segment);
        if (version != null) {
            segments.put(segment, segmentVersion);
        } else {
            applyOnNext(n -> n.activateSegmentVersion(segment, segmentVersion));
        }
    }

    protected Set<Integer> versions(long segment, int lastVersion) {
        Set<Integer> versions = new HashSet<>();
        for (int i = 0; i <= lastVersion; i++) {
            if (dataFile(new FileVersion(segment, i)).exists()) {
                versions.add(i);
            }
        }
        return versions;
    }


    public Stream<Long> allSegments() {
        return Stream.concat(getSegments().stream(), invokeOnNext(StorageTier::allSegments, Stream.empty()));
    }

    public void initSegments(long lastInitialized) {
        prepareSegmentStore(lastInitialized);
        applyOnNext(n -> n.initSegments(segments.isEmpty() ? lastInitialized : segments.navigableKeySet().last()));
        initialized.set(true);
    }


    protected Map<Long, Integer> prepareSegmentStore(long lastInitialized) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File events = new File(storagePath);
        FileUtils.checkCreateDirectory(events);
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());
        Arrays.stream(eventFiles)
              .map(FileUtils::process)
              .filter(segment -> segment.segment() < lastInitialized)
              .forEach(segment -> segments.compute(segment.segment(), (s, old) -> {
                  if (old == null) {
                      return segment.segmentVersion();
                  }
                  return Math.max(old, segment.segmentVersion());
              }));

        segments.forEach((segment, segmentVersion) -> renameFileIfNecessary(segment));
        return segments;
    }

    public SortedSet<FileVersion> segmentsWithoutIndex() {
        long firstValidIndex = segments.entrySet()
                                       .stream()
                                       .filter(entry -> indexManager.validIndex(new FileVersion(
                                               entry.getKey(),
                                               entry.getValue())))
                                       .map(Map.Entry::getKey)
                                       .findFirst().orElse(-1L);
        logger.info("{}: {} First valid index: {}", eventTypeContext, storagePath, firstValidIndex);
        TreeSet<FileVersion> local = segments.headMap(firstValidIndex)
                                             .entrySet()
                                             .stream()
                                             .map(e -> new FileVersion(e.getKey(), e.getValue()))
                                             .collect(Collectors.toCollection(TreeSet::new));
        if (firstValidIndex < 0) {
            local.addAll(invokeOnNext(StorageTier::segmentsWithoutIndex, Collections.emptySortedSet()));
        }
        return local;
    }


    public Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp) {
        Set<File> filenames = new TreeSet<>();
        getSegments().stream()
                     .filter(s -> (s > lastSegmentBackedUp
                             || currentSegmentVersion(s)
                             > lastVersionBackedUp))
                     .forEach(s -> versions(s, currentSegmentVersion(s))
                             .stream().filter(version -> s > lastSegmentBackedUp || version > lastVersionBackedUp)
                             .forEach(version -> {
                                 FileVersion segment = new FileVersion(s, version);
                                 filenames.add(dataFile(segment));
                                 filenames.addAll(indexManager.indexFiles(segment));
                             }));
        filenames.addAll(indexManager.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp)
                                     .collect(Collectors.toSet()));

        StorageTier nextStorageTier = nextSegmentsHandler.get();
        if (nextStorageTier == null) {
            return filenames.stream().map(File::getAbsolutePath);
        }

        return Stream.concat(filenames.stream().map(File::getAbsolutePath),
                             nextStorageTier.getBackupFilenames(lastSegmentBackedUp,
                                                                lastVersionBackedUp));
    }

    protected void renameFileIfNecessary(long segment) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File dataFile = storageProperties.oldDataFile(storagePath, segment);
        FileVersion fileVersion = new FileVersion(segment, 0);
        if (dataFile.exists()) {
            if (!dataFile.renameTo(dataFile(fileVersion))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(dataFile,
                                                                   dataFile(fileVersion)));
            }
            File indexFile = storageProperties.oldIndex(storagePath, segment);
            if (indexFile.exists() && !indexFile.renameTo(storageProperties.index(storagePath, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(indexFile,
                                                                   storageProperties.index(storagePath, segment)));
            }
            File bloomFile = storageProperties.oldBloomFilter(storagePath, segment);
            if (bloomFile.exists() && !bloomFile.renameTo(storageProperties.bloomFilter(storagePath, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(bloomFile,
                                                                   storageProperties.bloomFilter(storagePath,
                                                                                                 segment)));
            }
        }
    }

    protected File dataFile(FileVersion segment) {
        return storagePropertiesSupplier.get().dataFile(storagePath, segment);
    }


    private String renameMessage(File from, File to) {
        return "Could not rename " + from.getAbsolutePath() + " to " + to.getAbsolutePath();
    }

    /**
     * Gets an EventSource for the segment from this handler.
     *
     * @param segment the segment number and version of this segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    protected abstract Optional<EventSource> localEventSource(FileVersion segment);

    /**
     * Gets an EventSource for the segment from this handler.
     *
     * @param segment the segment number of this segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    protected abstract Optional<EventSource> localEventSource(long segment);


    //Retrieves event source from first available layer that is responsible for given segment
    public Optional<EventSource> eventSource(FileVersion segment) {
        Optional<EventSource> eventSource = localEventSource(segment);
        if (eventSource.isPresent()) {
            return eventSource;
        }
        return invokeOnNext(nextStorageTier -> nextStorageTier.eventSource(segment), Optional.empty());
    }

    public Optional<EventSource> eventSource(long segment) {
        Optional<EventSource> eventSource = localEventSource(segment);
        if (eventSource.isPresent()) {
            return eventSource;
        }
        return invokeOnNext(nextStorageTier -> nextStorageTier.eventSource(segment), Optional.empty());
    }

    /**
     * Get all segments
     *
     * @return descending set of segment ids
     */
    public SortedSet<Long> getSegments() {
        if (!initialized.get()) {
            initSegments(Long.MAX_VALUE);
        }
        return segments.navigableKeySet();
    }

    protected EventTypeContext getType() {
        return eventTypeContext;
    }

    protected void applyOnNext(Consumer<StorageTier> action) {
        StorageTier nextTier = nextSegmentsHandler.get();
        if (nextTier != null) {
            action.accept(nextTier);
        }
    }

    protected <R> R invokeOnNext(Function<StorageTier, R> action, R defaultValue) {
        StorageTier nextTier = nextSegmentsHandler.get();
        if (nextTier != null) {
            return action.apply(nextTier);
        }
        return defaultValue;
    }
}
