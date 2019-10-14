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
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
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

    private static final int TRANSACTION_LENGTH_BYTES = 4;
    private static final int NUMBER_OF_EVENTS_BYTES = 2;
    static final int VERSION_BYTES = 1;
    static final int FILE_OPTIONS_BYTES = 4;
    static final int TX_CHECKSUM_BYTES = 4;
    static final int HEADER_BYTES = TRANSACTION_LENGTH_BYTES + VERSION_BYTES + NUMBER_OF_EVENTS_BYTES;
    static final byte VERSION = 2;
    static final byte TRANSACTION_VERSION = 2;
    protected final String context;
    protected final IndexManager indexManager;
    protected final StorageProperties storageProperties;
    protected volatile SegmentBasedEventStore next;
    private final EventTypeContext type;
    protected final Set<Runnable> closeListeners = new CopyOnWriteArraySet<>();

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager, StorageProperties storageProperties) {
        this.type = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storageProperties = storageProperties;
    }

    private boolean foundFirstSequenceNumber(SortedSet<PositionInfo> positionInfos, long minSequenceNumber) {
        return !positionInfos.isEmpty() && positionInfos.first().getAggregateSequenceNumber() <= minSequenceNumber;
    }

    public abstract void handover(Long segment, Runnable callback);

    public void next(SegmentBasedEventStore datafileManager) {
        SegmentBasedEventStore last = this;
        while(last.next != null) {
            last = last.next;
        }
        last.next = datafileManager;
    }

    private Optional<PositionInfo> getLastPosition(String aggregateIdentifier, int maxSegments) {
        Comparator<PositionInfo> writePositionComparator = Comparator
                .comparingLong(PositionInfo::getAggregateSequenceNumber);
        return
                getSegments().stream()
                             .limit(maxSegments)
                             .map(segment -> {
                                    SortedSet<PositionInfo> positions = getPositions(segment, aggregateIdentifier);

                                    if (positions == null || positions.isEmpty()) return null;

                                    return positions.stream().max(writePositionComparator).get();
        }).filter(Objects::nonNull).findFirst();
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long firstSequenceNumber, Consumer<SerializedEvent> eventConsumer) {
        SortedMap<Long, SortedSet<PositionInfo>> positionInfos = getPositionInfos(aggregateId, firstSequenceNumber);
        boolean delegate = true;
        if( ! positionInfos.isEmpty()) {
            SortedSet<PositionInfo> first = positionInfos.get(positionInfos.firstKey());
            if( foundFirstSequenceNumber(first, firstSequenceNumber)) delegate = false;
        }

        if( delegate && next != null) {
            next.processEventsPerAggregate(aggregateId, firstSequenceNumber, eventConsumer);
        }

        positionInfos.keySet()
                     .forEach(segment -> retrieveEventsForAnAggregate(segment,
                                                                          positionInfos.get(segment),
                                                                          eventConsumer));

    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long firstSequenceNumber, long maxSequenceNumber,
                                          int maxResults, Consumer<SerializedEvent> eventConsumer) {
        SortedMap<Long, SortedSet<PositionInfo>> positionInfos = getPositionInfos(aggregateId, firstSequenceNumber, maxSequenceNumber, maxResults);
        AtomicInteger toDo = new AtomicInteger(maxResults);
        if( ! positionInfos.isEmpty()) {
            SortedSet<PositionInfo> first = positionInfos.get(positionInfos.firstKey());
            positionInfos.keySet().forEach(segment -> retrieveEventsForAnAggregate(segment,
                                                                          positionInfos.get(segment),
                                                                          e -> {
                                                                                if( toDo.getAndDecrement() > 0) {
                                                                                    eventConsumer.accept(e);
                                                                                }
                                                                          }));
            if( foundFirstSequenceNumber(first, firstSequenceNumber)) toDo.set(0);
        }

        if( toDo.get() > 0 && next != null) {
            next.processEventsPerAggregate(aggregateId, firstSequenceNumber, maxSequenceNumber, toDo.get(), eventConsumer);
        }



    }

    @Override
    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        for( long segment: getSegments() ) {
            Optional<EventSource> eventSource = getEventSource(segment);
            AtomicBoolean done = new AtomicBoolean();
            eventSource.ifPresent(e -> {
                long minTimestampInSegment = Long.MAX_VALUE;
                EventInformation eventWithToken;
                EventIterator iterator = createEventIterator(e,segment,segment);
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
                if (minToken > segment || minTimestampInSegment < minTimestamp) done.set(true);
                iterator.close();
            });
            if( done.get()) return;
        }

        if( next != null) {
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
        Optional<PositionInfo> positionInfo = getLastPosition(aggregateIdentifier, maxSegments);

        if( positionInfo.isPresent()) return Optional.of(positionInfo.get().getAggregateSequenceNumber());

        if( next != null && maxSegments > getSegments().size() ) {
            return next.getLastSequenceNumber(aggregateIdentifier, maxSegments - getSegments().size());
        }
        return Optional.empty();
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber) {
        for (Long segment : getSegments()) {
            SortedSet<PositionInfo> positionInfos = getPositions(segment, aggregateId);
            if (positionInfos != null && !positionInfos.isEmpty()) {
                PositionInfo last = positionInfos.last();
                if( last.getAggregateSequenceNumber() < minSequenceNumber) {
                    return Optional.empty();
                }
                Optional<EventSource> buffer = getEventSource(segment);
                if( buffer.isPresent()) {
                    SerializedEvent event = buffer.get().readEvent(last.getPosition());
                    buffer.get().close();
                    return Optional.of(event);
                }
            }
        }
        if( next != null)
            return next.getLastEvent(aggregateId, minSequenceNumber);

        return Optional.empty();
    }

    @Override
    public void init(boolean validate) {
        initSegments(Long.MAX_VALUE);
        validate(validate ? storageProperties.getValidationSegments() : 2);
    }


    @Override
    public long getFirstToken() {
        if( next != null) return next.getFirstToken();
        if( getSegments().isEmpty() ) return -1;
        return getSegments().last();
    }

    @Override
    public long getTokenAt(long instant) {
        for( long segment: getSegments() ) {
            Optional<EventSource> eventSource = getEventSource(segment);
            Long found = eventSource.map(es -> {
                EventIterator iterator = createEventIterator(es, segment, segment);
                Long token =  iterator.getTokenAfter(instant);
                if( token != null) {
                    iterator.close();
                }
                return token;
            }).orElse(null);
            if( found != null) {
                return found;
            }
        }

        if( next != null) return next.getTokenAt(instant);
        return -1;
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
        throw new UnsupportedOperationException("Operation only supported on primary event store");
    }

    public void validate(int maxSegments) {
        Stream<Long> segments = getAllSegments();
        List<ValidationResult> resultList = segments.limit(maxSegments).parallel().map(this::validateSegment).collect(Collectors.toList());
        resultList.stream().filter(validationResult ->  !validationResult.isValid()).findFirst().ifPresent(validationResult -> {
            throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED, validationResult.getMessage());
        });
        resultList.sort(Comparator.comparingLong(ValidationResult::getSegment));
        for( int i = 0 ; i < resultList.size() - 1 ; i++) {
            ValidationResult thisResult = resultList.get(i);
            ValidationResult nextResult = resultList.get(i+1);
            if( thisResult.getLastToken() != nextResult.getSegment()) {
                throw new MessagingPlatformException(ErrorCode.VALIDATION_FAILED, String.format("Validation exception: segment %d ending at %d", thisResult.getSegment(), thisResult.getLastToken()));
            }
        }
    }

    private Stream<Long> getAllSegments() {
        if( next == null) return getSegments().stream();
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

    protected TransactionIterator createTransactionIterator(EventSource eventSource, long segment, long token, boolean validating) {
        return eventSource.createTransactionIterator(segment, token, validating);
    }

    public long getSegmentFor(long token) {
        return getSegments().stream()
                            .filter( segment ->segment <= token)
                            .findFirst()
                            .orElse(next == null ? -1 : next.getSegmentFor(token));
    }

    @Override
    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return new TransactionWithTokenIterator(firstToken, limitToken);
    }

    protected SortedSet<Long> prepareSegmentStore(long lastInitialized) {
        SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
        File events  = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(events);
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());
        Arrays.stream(eventFiles)
              .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
              .filter(segment -> segment < lastInitialized)
              .forEach(segments::add);

        segments.forEach(this::renameFileIfNecessary);
        long firstValidIndex = segments.stream().filter(this::indexValid).findFirst().orElse(-1L);
        logger.debug("First valid index: {}", firstValidIndex);
        return segments;
    }

    private boolean indexValid(long segment) {
        if( indexManager.validIndex(segment)) {
            return true;
        }

        recreateIndex(segment);
        return false;
    }

    protected abstract void recreateIndex(long segment);

    private SortedMap<Long, SortedSet<PositionInfo>> getPositionInfos(String aggregateId, long minSequenceNumber) {
        final SortedMap<Long, SortedSet<PositionInfo>> result = new ConcurrentSkipListMap<>();
        for( long segment : getSegments()) {
            SortedSet<PositionInfo> positionInfos = getPositions(segment, aggregateId);
            if (positionInfos != null) {
                positionInfos = positionInfos.tailSet(new PositionInfo(0, minSequenceNumber));
                if (!positionInfos.isEmpty())
                    result.put(segment, positionInfos);

                if (foundFirstSequenceNumber(positionInfos, minSequenceNumber)) {
                    return result;
                }
            }
        }

        return result;
    }

    // Returns a map of segmentNr, positions for the aggregate with sequence number between minSequenceNumber and maxSequenceNumber (inclusive) and at most maxResults
    // finds the highest matching sequence numbers
    private SortedMap<Long, SortedSet<PositionInfo>> getPositionInfos(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults) {
        final SortedMap<Long, SortedSet<PositionInfo>> result = new ConcurrentSkipListMap<>((k1,k2) -> Long.compare(k2, k1));
        long actualMax = maxSequenceNumber < Long.MAX_VALUE ? maxSequenceNumber + 1 : Long.MAX_VALUE;
        int toDo = maxResults;
        for( long segment : getSegments()) {
            SortedSet<PositionInfo> positionInfos = getPositions(segment, aggregateId);
            if (positionInfos != null) {
                boolean minInSet = foundFirstSequenceNumber(positionInfos, minSequenceNumber);
                positionInfos = positionInfos.subSet(new PositionInfo(0, minSequenceNumber), new PositionInfo(0, actualMax));
                if (!positionInfos.isEmpty()) {
                    result.put(segment, reverse(positionInfos));
                    toDo -= positionInfos.size();
                }

                if (minInSet || toDo <= 0) {
                    return result;
                }
            }
        }

        return result;
    }

    private SortedSet<PositionInfo> reverse(SortedSet<PositionInfo> positionInfos) {
        SortedSet<PositionInfo> reversedPositionInfos = new TreeSet<>(Collections.reverseOrder());
        reversedPositionInfos.addAll(positionInfos);
        return reversedPositionInfos;
    }

    private void retrieveEventsForAnAggregate(long segment, SortedSet<PositionInfo> positions, Consumer<SerializedEvent> onEvent) {
        Optional<EventSource> buffer = getEventSource(segment);
        buffer.ifPresent(eventSource -> {
            positions.forEach(positionInfo -> onEvent.accept(eventSource.readEvent(positionInfo.getPosition())));
            eventSource.close();
        });
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        Stream<String> filenames = getSegments().stream().filter(s -> s > lastSegmentBackedUp).map( s -> Stream.of(
                storageProperties.dataFile(context, s).getAbsolutePath(),
                storageProperties.index(context, s).getAbsolutePath(),
                storageProperties.bloomFilter(context, s).getAbsolutePath()
        )).flatMap(Function.identity());
        if( next == null) return filenames;
        return Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp));
    }

    @Override
    public void health(Health.Builder builder) {
        String storage = storageProperties.getStorage(context);
        SegmentBasedEventStore n = next;
        Path path = Paths.get(storage);
        try {
            FileStore store = Files.getFileStore(path);
            builder.withDetail(context + ".free",store.getUsableSpace());
            builder.withDetail(context + ".path",path.toString());
        } catch (IOException e) {
            logger.warn("Failed to retrieve filestore for {}", path, e);
        }
        while( n != null) {
            if( ! storage.equals(next.storageProperties.getStorage(context))) {
                n.health(builder);
                return;
            }
            n = n.next;
        }
    }

    protected void renameFileIfNecessary(long segment) {
        File dataFile = storageProperties.oldDataFile(context, segment);
        if( dataFile.exists()) {
            if( ! dataFile.renameTo(storageProperties.dataFile(context, segment)) ) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Could not rename " + dataFile.getAbsolutePath() + " to " + storageProperties.dataFile(context, segment) );
            }
            File indexFile = storageProperties.oldIndex(context, segment);
            if( indexFile.exists() && ! indexFile.renameTo(storageProperties.index(context, segment)) ) {
                    throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Could not rename " + indexFile.getAbsolutePath() + " to " + storageProperties.index(context, segment) );
            }
            File bloomFile = storageProperties.oldBloomFilter(context, segment);
            if( bloomFile.exists() && ! bloomFile.renameTo(storageProperties.bloomFilter(context, segment))) {
                    throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Could not rename " + bloomFile.getAbsolutePath() + " to " + storageProperties.bloomFilter(context, segment) );
            }
        }
    }

    /**
     * @param segment gets an EventSource for the segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    protected abstract Optional<EventSource> getEventSource(long segment);

    /**
     * Get all segments
     * @return descending set of segment ids
     */
    protected abstract SortedSet<Long> getSegments();

    /**
     * @param segment the segment to search positions for the aggregate
     * @param aggregateId the aggregate id
     * @return sorted set of positions, ordered by sequenceNumber (ascending), or empty set if not found
     */
    protected abstract SortedSet<PositionInfo> getPositions(long segment, String aggregateId);

    @Override
    public byte transactionVersion() {
        return TRANSACTION_VERSION;
    }

    @Override
    public long nextToken() {
        return 0;
    }

    private class TransactionWithTokenIterator implements Iterator<SerializedTransactionWithToken> {

        private long currentToken;
        private final Long limitToken;
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
