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
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.data.util.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.ArrayUtils.contains;

/**
 * @author Marc Gathier
 */
public abstract class SegmentBasedEventStore implements EventStorageEngine {

    protected static final Logger logger = LoggerFactory.getLogger(SegmentBasedEventStore.class);
    protected static final int MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK = 10;
    protected static final int VERSION_BYTES = 1;
    protected static final int FILE_OPTIONS_BYTES = 4;
    protected static final int TX_CHECKSUM_BYTES = 4;
    protected static final byte VERSION = 2;
    protected static final byte TRANSACTION_VERSION = 2;
    private static final int TRANSACTION_LENGTH_BYTES = 4;
    private static final int NUMBER_OF_EVENTS_BYTES = 2;
    protected static final int HEADER_BYTES = TRANSACTION_LENGTH_BYTES + VERSION_BYTES + NUMBER_OF_EVENTS_BYTES;
    protected final String context;
    protected final IndexManager indexManager;
    protected final StorageProperties storageProperties;
    protected final EventTypeContext type;
    protected final Set<Runnable> closeListeners = new CopyOnWriteArraySet<>();
    private final MeterFactory meterFactory;
    private final Timer aggregateReadTimer;
    private final Timer lastSequenceReadTimer;
    protected volatile SegmentBasedEventStore next;

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager,
                                  StorageProperties storageProperties, MeterFactory meterFactory) {
        this.type = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storageProperties = storageProperties;
        this.meterFactory = meterFactory;
        this.aggregateReadTimer = meterFactory.timer(BaseMetricName.AXON_AGGREGATE_READTIME,
                                                     Tags.of(MeterFactory.CONTEXT,
                                                             eventTypeContext.getContext(),
                                                             "type",
                                                             eventTypeContext.getEventType().toString()));
        this.lastSequenceReadTimer = meterFactory.timer(BaseMetricName.AXON_LAST_SEQUENCE_READTIME,
                                                        Tags.of(MeterFactory.CONTEXT,
                                                                eventTypeContext.getContext(),
                                                                "type",
                                                                eventTypeContext.getEventType().toString()));
    }

    public abstract void handover(Long segment, Runnable callback);

    public void next(SegmentBasedEventStore datafileManager) {
        SegmentBasedEventStore last = this;
        while (last.next != null) {
            last = last.next;
        }
        last.next = datafileManager;
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long firstSequenceNumber,
                                          Consumer<SerializedEvent> eventConsumer) {
        long before = System.currentTimeMillis();
        SortedMap<Long, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                   firstSequenceNumber,
                                                                                   Long.MAX_VALUE,
                                                                                   Long.MAX_VALUE,
                                                                                   EventType.SNAPSHOT
                                                                                           .equals(type.getEventType()));
        positionInfos.forEach((segment, positionInfo) -> retrieveEventsForAnAggregate(segment,
                                                                                      positionInfo.positions(),
                                                                                      firstSequenceNumber,
                                                                                      Long.MAX_VALUE,
                                                                                      eventConsumer,
                                                                                      Long.MAX_VALUE));
        aggregateReadTimer.record(System.currentTimeMillis() - before, TimeUnit.MILLISECONDS);
    }

    @Override
    public void processEventsPerAggregateHighestFirst(String aggregateId, long firstSequenceNumber,
                                                      long maxSequenceNumber,
                                                      int maxResults, Consumer<SerializedEvent> eventConsumer) {
        SortedMap<Long, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                   firstSequenceNumber,
                                                                                   maxSequenceNumber,
                                                                                   maxResults,
                                                                                   EventType.SNAPSHOT
                                                                                           .equals(type.getEventType()));

        List<Long> segmentsContainingAggregate = new ArrayList<>(positionInfos.keySet());
        Collections.reverse(segmentsContainingAggregate);
        for (Long segmentContainingAggregate : segmentsContainingAggregate) {
            IndexEntries entries = positionInfos.get(segmentContainingAggregate);
            List<IndexEntry> positions = new ArrayList<>(entries.positions());
            Collections.reverse(positions);
            maxResults -= retrieveEventsForAnAggregate(segmentContainingAggregate,
                                                       positions,
                                                       firstSequenceNumber,
                                                       maxSequenceNumber,
                                                       eventConsumer,
                                                       maxResults);
            if (maxResults <= 0) {
                return;
            }
        }
    }


    @Override
    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        for (long segment : getSegments()) {
            Optional<EventSource> eventSource = getEventSource(segment);
            AtomicBoolean done = new AtomicBoolean();
            eventSource.ifPresent(e -> {
                long minTimestampInSegment = Long.MAX_VALUE;
                EventInformation eventWithToken;
                EventIterator iterator = createEventIterator(e, segment, segment);
                while (iterator.hasNext()) {
                    eventWithToken = iterator.next();
                    minTimestampInSegment = Math.min(minTimestampInSegment, eventWithToken.getEvent().getTimestamp());
                    if (eventWithToken.getToken() >= minToken
                            && eventWithToken.getEvent().getTimestamp() >= minTimestamp
                            && !consumer.test(eventWithToken.asEventWithToken())) {
                        iterator.close();
                        return;
                    }
                }
                if (minToken > segment || minTimestampInSegment < minTimestamp) {
                    done.set(true);
                }
                iterator.close();
            });
            if (done.get()) {
                return;
            }
        }

        if (next != null) {
            next.query(minToken, minTimestamp, consumer);
        }
    }

    protected EventIterator createEventIterator(EventSource e, long segment, long startToken) {
        return e.createEventIterator(segment, startToken);
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint[] hints) {
        return getLastSequenceNumber(aggregateIdentifier, contains(hints, SearchHint.RECENT_ONLY) ?
                MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK : Integer.MAX_VALUE);
    }

    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, int maxSegments) {
        long before = System.currentTimeMillis();
        try {
            return indexManager.getLastSequenceNumber(aggregateIdentifier, maxSegments);
        } finally {
            lastSequenceReadTimer.record(System.currentTimeMillis() - before, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber) {
        SegmentAndPosition lastEventPosition = indexManager.lastEvent(aggregateId, minSequenceNumber);
        if (lastEventPosition == null) {
            return Optional.empty();
        }

        return readSerializedEvent(minSequenceNumber, lastEventPosition);
    }

    private Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber,
                                                          SegmentAndPosition lastEventPosition) {
        Optional<EventSource> eventSource = getEventSource(lastEventPosition.getSegment());
        if (eventSource.isPresent()) {
            return readSerializedEvent(eventSource.get(), minSequenceNumber, lastEventPosition.getPosition());
        }

        if (next != null) {
            next.readSerializedEvent(minSequenceNumber, lastEventPosition);
        }

        return Optional.empty();
    }

    private Optional<SerializedEvent> readSerializedEvent(EventSource eventSource, long minSequenceNumber,
                                                          int lastEventPosition) {
        SerializedEvent event = eventSource.readEvent(lastEventPosition);
        if (event.getAggregateSequenceNumber() >= minSequenceNumber) {
            return Optional.of(event);
        }
        return Optional.empty();
    }

    @Override
    public void init(boolean validate) {
        initSegments(Long.MAX_VALUE);
        validate(validate ? storageProperties.getValidationSegments() : 2);
    }


    @Override
    public long getFirstToken() {
        if (next != null) {
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
                EventIterator iterator = createEventIterator(es, segment, segment);
                Long token = iterator.getTokenAfter(instant);
                if (token != null) {
                    iterator.close();
                }
                return token;
            }).orElse(null);
            if (found != null) {
                return found;
            }
        }

        if (next != null) {
            return next.getTokenAt(instant);
        }
        return -1;
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
        throw new UnsupportedOperationException("Operation only supported on primary event store");
    }

    public void validate(int maxSegments) {
        Stream<Long> segments = getAllSegments();
        List<ValidationResult> resultList = segments.limit(maxSegments).parallel().map(this::validateSegment).collect(
                Collectors.toList());
        resultList.stream().filter(validationResult -> !validationResult.isValid()).findFirst().ifPresent(
                validationResult -> {
                    throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED, validationResult.getMessage());
                });
        resultList.sort(Comparator.comparingLong(ValidationResult::getSegment));
        for (int i = 0; i < resultList.size() - 1; i++) {
            ValidationResult thisResult = resultList.get(i);
            ValidationResult nextResult = resultList.get(i + 1);
            if (thisResult.getLastToken() != nextResult.getSegment()) {
                throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED,
                                                     String.format(
                                                             "Validation exception: segment %d ending at token, %d, next segment starts at token %d",
                                                             thisResult.getSegment(),
                                                             thisResult.getLastToken() - 1,
                                                             nextResult.getSegment()));
            }
        }
    }

    private Stream<Long> getAllSegments() {
        if (next == null) {
            return getSegments().stream();
        }
        return Stream.concat(getSegments().stream(), next.getSegments().stream()).distinct();
    }

    protected ValidationResult validateSegment(long segment) {
        logger.debug("{}: Validating {} segment: {}", type.getContext(), type.getEventType(), segment);
        try (TransactionIterator iterator = getTransactions(segment, segment, true)) {
            SerializedTransactionWithToken last = null;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            return new ValidationResult(segment, last == null ? segment : last.getToken() + last.getEvents().size());
        } catch (Exception ex) {
            return new ValidationResult(segment, ex.getMessage());
        }
    }

    @Override
    public EventTypeContext getType() {
        return type;
    }

    @Override
    public Registration registerCloseListener(Runnable listener) {
        closeListeners.add(listener);
        return () -> closeListeners.remove(listener);
    }

    public abstract void initSegments(long maxValue);

    public EventIterator getEvents(long segment, long token) {
        Optional<EventSource> reader = getEventSource(segment);
        return reader.map(eventSource -> createEventIterator(eventSource, segment, token))
                     .orElseGet(() -> next.getEvents(segment, token));
    }

    public TransactionIterator getTransactions(long segment, long token) {
        return getTransactions(segment, token, false);
    }

    protected TransactionIterator getTransactions(long segment, long token, boolean validating) {
        Optional<EventSource> reader = getEventSource(segment);
        return reader.map(r -> createTransactionIterator(r, segment, token, validating))
                     .orElseGet(() -> next.getTransactions(segment, token, validating));
    }

    protected TransactionIterator createTransactionIterator(EventSource eventSource, long segment, long token,
                                                            boolean validating) {
        return eventSource.createTransactionIterator(segment, token, validating);
    }

    public long getSegmentFor(long token) {
        return getSegments().stream()
                            .filter(segment -> segment <= token)
                            .findFirst()
                            .orElse(next == null ? -1 : next.getSegmentFor(token));
    }

    @Override
    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return new TransactionWithTokenIterator(firstToken, limitToken);
    }

    protected SortedSet<Long> prepareSegmentStore(long lastInitialized) {
        SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
        File events = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(events);
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());
        Arrays.stream(eventFiles)
              .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
              .filter(segment -> segment < lastInitialized)
              .forEach(segments::add);

        segments.forEach(this::renameFileIfNecessary);
        long firstValidIndex = segments.stream().filter(indexManager::validIndex).findFirst().orElse(-1L);
        logger.warn("First valid index: {}", firstValidIndex);
        SortedSet<Long> recreate = new TreeSet<>();
        recreate.addAll(segments.headSet(firstValidIndex));
        recreate.forEach(this::recreateIndex);
        return segments;
    }

    protected abstract void recreateIndex(long segment);

    private int retrieveEventsForAnAggregate(long segment, List<IndexEntry> indexEntries, long minSequenceNumber,
                                             long maxSequenceNumber,
                                             Consumer<SerializedEvent> onEvent, long maxResults) {
        Optional<EventSource> buffer = getEventSource(segment);
        int processed = 0;

        if (buffer.isPresent()) {
            EventSource eventSource = buffer.get();
            for (int i = 0; i < indexEntries.size() && i < maxResults; i++) {
                SerializedEvent event = eventSource.readEvent(indexEntries.get(i).getPosition());
                if (event.getAggregateSequenceNumber() >= minSequenceNumber
                        && event.getAggregateSequenceNumber() < maxSequenceNumber) {
                    onEvent.accept(event);
                }
                processed++;
            }
        } else {
            if (next != null) {
                processed = next.retrieveEventsForAnAggregate(segment,
                                                              indexEntries,
                                                              minSequenceNumber,
                                                              maxSequenceNumber,
                                                              onEvent,
                                                              maxResults);
            }
        }

        return processed;
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        Stream<String> filenames = getSegments().stream()
                                                .filter(s -> s > lastSegmentBackedUp)
                                                .flatMap(s -> Stream.of(
                                                        storageProperties.dataFile(context, s).getAbsolutePath(),
                                                        storageProperties.index(context, s).getAbsolutePath(),
                                                        storageProperties.bloomFilter(context, s).getAbsolutePath()
                                                ));
        if (next == null) {
            return filenames;
        }
        return Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp));
    }

    @Override
    public void health(Health.Builder builder) {
        String storage = storageProperties.getStorage(context);
        SegmentBasedEventStore n = next;
        Path path = Paths.get(storage);
        try {
            FileStore store = Files.getFileStore(path);
            builder.withDetail(context + ".free", store.getUsableSpace());
            builder.withDetail(context + ".path", path.toString());
        } catch (IOException e) {
            logger.warn("Failed to retrieve filestore for {}", path, e);
        }
        while (n != null) {
            if (!storage.equals(next.storageProperties.getStorage(context))) {
                n.health(builder);
                return;
            }
            n = n.next;
        }
    }

    protected void renameFileIfNecessary(long segment) {
        File dataFile = storageProperties.oldDataFile(context, segment);
        if (dataFile.exists()) {
            if (!dataFile.renameTo(storageProperties.dataFile(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     "Could not rename " + dataFile.getAbsolutePath() + " to "
                                                             + storageProperties.dataFile(context, segment));
            }
            File indexFile = storageProperties.oldIndex(context, segment);
            if (indexFile.exists() && !indexFile.renameTo(storageProperties.index(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     "Could not rename " + indexFile.getAbsolutePath() + " to "
                                                             + storageProperties.index(context, segment));
            }
            File bloomFile = storageProperties.oldBloomFilter(context, segment);
            if (bloomFile.exists() && !bloomFile.renameTo(storageProperties.bloomFilter(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     "Could not rename " + bloomFile.getAbsolutePath() + " to "
                                                             + storageProperties.bloomFilter(context, segment));
            }
        }
    }

    protected void recreateIndexFromIterator(long segment, EventIterator iterator) {
        while (iterator.hasNext()) {
            EventInformation event = iterator.next();
            if (event.isDomainEvent()) {
                indexManager.addToActiveSegment(segment, event.getEvent().getAggregateIdentifier(), new IndexEntry(
                        event.getEvent().getAggregateSequenceNumber(),
                        event.getPosition(),
                        event.getToken()));
//                aggregatePositions.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
//                                                   k -> new ConcurrentSkipListSet<>())
//                                  .add(new PositionInfo(event.getPosition(),
//                                                        event.getEvent().getAggregateSequenceNumber()));
            }
        }
        indexManager.complete(segment);
    }

    /**
     * @param segment gets an EventSource for the segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    public abstract Optional<EventSource> getEventSource(long segment);

    /**
     * Get all segments
     *
     * @return descending set of segment ids
     */
    protected abstract SortedSet<Long> getSegments();

    /**
     * @param segment     the segment to search positions for the aggregate
     * @param aggregateId the aggregate id
     * @return sorted set of positions, ordered by sequenceNumber (ascending), or empty set if not found
     */
    protected IndexEntries getPositions(long segment, String aggregateId) {
        return indexManager.positions(segment, aggregateId);
    }

    @Override
    public byte transactionVersion() {
        return TRANSACTION_VERSION;
    }

    @Override
    public long nextToken() {
        return 0;
    }

    private class TransactionWithTokenIterator implements Iterator<SerializedTransactionWithToken> {

        private final Long limitToken;
        private long currentToken;
        private long currentSegment;
        private TransactionIterator currentTransactionIterator;

        TransactionWithTokenIterator(long token) {
            this(token, null);
        }

        TransactionWithTokenIterator(long token, Long limitToken) {
            this.currentToken = token;
            this.limitToken = limitToken;
            this.currentSegment = getSegmentFor(token);
            this.currentTransactionIterator = getTransactions(currentSegment, currentToken);
        }

        @Override
        public boolean hasNext() {
            return currentTransactionIterator.hasNext();
        }

        @Override
        public SerializedTransactionWithToken next() {
            SerializedTransactionWithToken next = currentTransactionIterator.next();
            currentToken += next.getEventsCount();
            checkPointers();
            return next;
        }

        private void checkPointers() {
            if (limitToken != null && currentToken >= limitToken) {
                // we are done
                currentTransactionIterator.close();
            } else if (!currentTransactionIterator.hasNext()) {
                currentSegment = getSegmentFor(currentToken);
                currentTransactionIterator.close();
                currentTransactionIterator = getTransactions(currentSegment, currentToken);
            }
        }
    }
}
