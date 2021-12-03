/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.EventStoreValidationException;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventTransformationFunction;
import io.axoniq.axonserver.localstorage.EventTransformationResult;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class SegmentBasedEventStore implements EventStorageEngine {

    public static final byte TRANSACTION_VERSION = 2;
    protected static final Logger logger = LoggerFactory.getLogger(SegmentBasedEventStore.class);
    protected static final int MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK = 10;
    protected static final int VERSION_BYTES = 1;
    protected static final int FILE_OPTIONS_BYTES = 4;
    protected static final int TX_CHECKSUM_BYTES = 4;
    protected static final byte VERSION = 2;
    private static final int TRANSACTION_LENGTH_BYTES = 4;
    private static final int NUMBER_OF_EVENTS_BYTES = 2;
    protected static final int HEADER_BYTES = TRANSACTION_LENGTH_BYTES + VERSION_BYTES + NUMBER_OF_EVENTS_BYTES;
    protected static final int FILE_HEADER_SIZE = VERSION_BYTES + FILE_OPTIONS_BYTES;
    protected static final int FILE_FOOTER_SIZE = 4;
    protected static final int MAX_TRANSACTION_SIZE = Integer.MAX_VALUE - FILE_HEADER_SIZE - FILE_FOOTER_SIZE;
    protected final String context;
    protected final IndexManager indexManager;
    protected final StorageProperties storageProperties;
    protected final EventTypeContext type;
    protected final Set<Runnable> closeListeners = new CopyOnWriteArraySet<>();
    private final Timer lastSequenceReadTimer;
    protected final SegmentBasedEventStore next;
    private static final int PREFETCH_SEGMENT_FILES = 2;
    protected final Counter fileOpenMeter;
    private final DistributionSummary aggregateSegmentsCount;

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager,
                                  StorageProperties storageProperties, MeterFactory meterFactory) {
        this(eventTypeContext, indexManager, storageProperties, null, meterFactory);
    }

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager,
                                  StorageProperties storageProperties,
                                  SegmentBasedEventStore nextSegmentsHandler,
                                  MeterFactory meterFactory) {
        this.type = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storageProperties = storageProperties;
        this.next = nextSegmentsHandler;
        Tags tags = Tags.of(MeterFactory.CONTEXT, context, "type", eventTypeContext.getEventType().name());
        this.fileOpenMeter = meterFactory.counter(BaseMetricName.AXON_SEGMENT_OPEN, tags);
        this.lastSequenceReadTimer = meterFactory.timer(BaseMetricName.AXON_LAST_SEQUENCE_READTIME, tags);
        this.aggregateSegmentsCount = meterFactory.distributionSummary
                                                          (BaseMetricName.AXON_AGGREGATE_SEGMENT_COUNT, tags);
    }

    public abstract void handover(FileVersion segment, Runnable callback);

    public Flux<SerializedEvent> eventsPerAggregate(String aggregateId,
                                                    long firstSequence,
                                                    long lastSequence,
                                                    long minToken) {
        logger.debug("Reading index entries for aggregate {} started.", aggregateId);
        //Map<segment, all positions in that segment>
        SortedMap<FileVersion, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                          firstSequence,
                                                                                          lastSequence,
                                                                                          Long.MAX_VALUE,
                                                                                          minToken);
        logger.debug("Reading index entries for aggregate {} finished.", aggregateId);
        aggregateSegmentsCount.record(positionInfos.size());

        return Flux.fromIterable(positionInfos.entrySet())
                   .flatMapSequential(e -> eventsForPositions(e.getKey(), e.getValue()),
                                      PREFETCH_SEGMENT_FILES,
                                      storageProperties.getEventsPerSegmentPrefetch())
                   .skipUntil(se -> se.getAggregateSequenceNumber()
                           >= firstSequence) //todo for safe guard, remove in 4.6
                   .takeWhile(se -> se.getAggregateSequenceNumber() < lastSequence)
                   .name("event_stream")
                   .tag("context", context)
                   .tag("stream", "aggregate_events")
                   .tag("origin", "event_sources")
                   .metrics();
    }

    private Flux<SerializedEvent> eventsForPositions(FileVersion segment, IndexEntries indexEntries) {
        return (!containsSegment(segment.segment()) && next != null) ?
                next.eventsForPositions(segment, indexEntries) :
                new EventSourceFlux(indexEntries,
                                    () -> eventSource(segment),
                                    segment.segment(),
                                    storageProperties.getEventsPerSegmentPrefetch()).get()
                                                                                    .name("event_stream")
                                                                                    .tag("context", context)
                                                                                    .tag("stream", "aggregate_events")
                                                                                    .tag("origin", "event_source")
                                                                                    .metrics();
    }

    /**
     * Returns {@code true} if this instance is resposnsible to handling the specified segment, {@code false}
     * otherwise.
     *
     * @param segment the segment to check
     * @return {@code true} if this instance is resposnsible to handling the specified segment, {@code false} otherwise.
     */
    protected abstract boolean containsSegment(long segment);

    @Override
    public void processEventsPerAggregate(String aggregateId, long firstSequenceNumber, long lastSequenceNumber,
                                          long minToken, Consumer<SerializedEvent> eventConsumer) {
        SortedMap<FileVersion, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                          firstSequenceNumber,
                                                                                          lastSequenceNumber,
                                                                                          Long.MAX_VALUE,
                                                                                          minToken);
        positionInfos.forEach((segment, positionInfo) -> retrieveEventsForAnAggregate(segment,
                                                                                      positionInfo.positions(),
                                                                                      firstSequenceNumber,
                                                                                      lastSequenceNumber,
                                                                                      eventConsumer,
                                                                                      Long.MAX_VALUE,
                                                                                      minToken));
    }

    @Override
    public void processEventsPerAggregateHighestFirst(String aggregateId, long firstSequenceNumber,
                                                      long maxSequenceNumber,
                                                      int maxResults, Consumer<SerializedEvent> eventConsumer) {
        SortedMap<FileVersion, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                          firstSequenceNumber,
                                                                                          maxSequenceNumber,
                                                                                          maxResults,
                                                                                          0);

        List<FileVersion> segmentsContainingAggregate = new ArrayList<>(positionInfos.keySet());
        Collections.reverse(segmentsContainingAggregate);
        for (FileVersion segmentContainingAggregate : segmentsContainingAggregate) {
            IndexEntries entries = positionInfos.get(segmentContainingAggregate);
            List<Integer> positions = new ArrayList<>(entries.positions());
            Collections.reverse(positions);
            maxResults -= retrieveEventsForAnAggregate(segmentContainingAggregate,
                                                       positions,
                                                       firstSequenceNumber,
                                                       maxSequenceNumber,
                                                       eventConsumer,
                                                       maxResults, 0);
            if (maxResults <= 0) {
                return;
            }
        }
    }


    @Override
    public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {
        for (long segment : getSegments()) {
            if (segment <= queryOptions.getMaxToken()) {
                Optional<EventSource> eventSource = getEventSource(segment);
                AtomicBoolean done = new AtomicBoolean();
                boolean snapshot = EventType.SNAPSHOT.equals(type.getEventType());
                eventSource.ifPresent(e -> {
                    long minTimestampInSegment = Long.MAX_VALUE;
                    EventInformation eventWithToken;
                    EventIterator iterator = createEventIterator(e, segment, segment);
                    while (iterator.hasNext()) {
                        eventWithToken = iterator.next();
                        minTimestampInSegment = Math.min(minTimestampInSegment,
                                                         eventWithToken.getEvent().getTimestamp());
                        if (eventWithToken.getToken() > queryOptions.getMaxToken()) {
                            iterator.close();
                            return;
                        }
                        if (eventWithToken.getToken() >= queryOptions.getMinToken()
                                && eventWithToken.getEvent().getTimestamp() >= queryOptions.getMinTimestamp()
                                && !consumer.test(eventWithToken.asEventWithToken(snapshot))) {
                            iterator.close();
                            return;
                        }
                    }
                    if (queryOptions.getMinToken() > segment || minTimestampInSegment < queryOptions
                            .getMinTimestamp()) {
                        done.set(true);
                    }
                    iterator.close();
                    e.close();
                });
                if (done.get()) {
                    return;
                }
            }
        }

        if (next != null) {
            next.query(new QueryOptions(queryOptions.getMinToken(),
                                        queryOptions.getMaxToken(),
                                        queryOptions.getMinTimestamp()), consumer);
        }
    }

    protected abstract Optional<EventSource> getEventSource(long segment);

    protected EventIterator createEventIterator(EventSource e, long segment, long startToken) {
        return e.createEventIterator(segment, startToken);
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint[] hints) {
        return getLastSequenceNumber(aggregateIdentifier, contains(hints, SearchHint.RECENT_ONLY) ?
                MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK : Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    public void transformContents(long firstToken,
                                  long lastToken,
                                  boolean keepOldVersions,
                                  int newVersion,
                                  EventTransformationFunction transformationFunction,
                                  Consumer<TransformationProgress> transformationProgressConsumer) {
        long nextToken = Math.max(firstToken, getFirstToken());
        long previousSegment = -1;
        while (nextToken <= lastToken) {
            long segment = getSegmentFor(nextToken);
            if (segment == previousSegment) {
                return;
            }
            nextToken = transformSegment(segment,
                                         nextToken,
                                         keepOldVersions,
                                         newVersion,
                                         transformationFunction,
                                         transformationProgressConsumer);
            previousSegment = segment;
        }
    }

    @Override
    public void deleteOldVersions(int version) {
        SortedSet<Long> mySegments = getSegments();
        mySegments.forEach(s -> {
            if (storageProperties.dataFile(context, new FileVersion(s, version)).exists()) {
                int previousVersion = previousVersion(s, version);
                if( previousVersion >= 0) {
                    scheduleForDeletion(s, previousVersion);
                }
            }
        });
        if (next != null) {
            next.deleteOldVersions(version);
        }
    }

    @Override
    public void rollbackSegments(int version) {
        SortedSet<Long> mySegments = getSegments();
        mySegments.forEach(s -> {
            if (currentSegmentVersion(s) == version) {
                int previousVersion = previousVersion(s, version);
                if( previousVersion >= 0) {
                    indexManager.activeVersion(s, previousVersion);
                    activateSegmentVersion(s, previousVersion);
                    scheduleForDeletion(s, version);
                }
            }
        });

        if (next != null) {
            next.rollbackSegments(version);
        }
    }

    @Override
    public boolean canRollbackTransformation(int version, long firstEventToken, long lastEventToken) {
        if (lastEventToken < getFirstToken()) {
            return true;
        }
        if (getSegments().stream().anyMatch(s -> currentSegmentVersion(s) == version && hasPreviousVersion(s, version))) {
            return true;
        }

        if (next != null) {
            return next.canRollbackTransformation(version, firstEventToken, lastEventToken);
        }
        return false;
    }

    @Override
    public int nextVersion() {
        int version = 1;
        for (Long segment : getSegments()) {
           version = Math.max(version, currentSegmentVersion(segment)+1);
        }
        return next == null ? version : Math.max(version, next.nextVersion());
    }

    private boolean hasPreviousVersion(long segment, int version) {
        return previousVersion(segment, version) >= 0;
    }

    private int previousVersion(Long segment, int version) {
        int previous = version - 1;
        while (previous >= 0) {
            if (storageProperties.dataFile(context, new FileVersion(segment, previous)).exists()) {
                return previous;
            }
            previous--;
        }
        return -1;
    }

    private long transformSegment(long segment, long nextTokenToTransform, boolean keepOldVersion,
                                  int newVersion,
                                  EventTransformationFunction transformationFunction,
                                  Consumer<TransformationProgress> transformationProgressConsumer) {
        Map<String, List<IndexEntry>> indexEntriesMap;
        boolean changed = false;
        int currentVersion = currentSegmentVersion(segment);

        File dataFile = storageProperties.dataFile(context, new FileVersion(segment, newVersion));
        File tempFile = new File(dataFile.getAbsolutePath() + ".temp");
        long token;
        try (SegmentWriter segmentWriter = new StreamSegmentWriter(tempFile, segment, storageProperties.getFlags());
             TransactionIterator transactionIterator = getTransactions(segment, segment)) {

            while (transactionIterator.hasNext()) {
                SerializedTransactionWithToken transaction = transactionIterator.next();
                List<Event> updatedEvents = new ArrayList<>();
                int i = 0;
                for (SerializedEvent serializedEvent : transaction.getEvents()) {
                    Event event = serializedEvent.asEvent();
                    if (transaction.getToken() + i == nextTokenToTransform) {
                        EventTransformationResult result = transformationFunction.apply(event,
                                                                                        transaction.getToken() + i);
                        if (!event.equals(result.event())) {
                            changed = true;
                            event = result.event();
                        }
                        nextTokenToTransform = Math.max(nextTokenToTransform + 1, result.nextToken());
                    }
                    updatedEvents.add(event);
                    i++;
                }

                segmentWriter.write(updatedEvents);

            }
            segmentWriter.writeEndOfFile();
            token = segmentWriter.lastToken();
            indexEntriesMap = segmentWriter.indexEntries();
        } catch (Exception e) {
            FileUtils.delete(tempFile);
            throw new RuntimeException(String.format("%s: transformation of segment %d failed", context, segment), e);
        }
        if (changed) {
            completeNewSegmentVersion(segment,
                                      keepOldVersion,
                                      newVersion,
                                      indexEntriesMap,
                                      currentVersion,
                                      dataFile,
                                      tempFile);
        } else {
            FileUtils.delete(tempFile);
        }
        transformationProgressConsumer.accept(new TransformationProgressUpdate(token));
        return nextTokenToTransform;
    }

    private void completeNewSegmentVersion(long segment, boolean keepOldVersion, int newVersion,
                                           Map<String, List<IndexEntry>> indexEntriesMap, int currentVersion,
                                           File dataFile,
                                           File tempFile) {
        indexManager.createNewVersion(segment, newVersion, indexEntriesMap);
        try {
            FileUtils.rename(tempFile, dataFile);
            activateSegmentVersion(segment, newVersion);
            if (!keepOldVersion) {
                scheduleForDeletion(segment, currentVersion);
            }
        } catch (IOException e) {
            indexManager.remove(new FileVersion(segment, newVersion));
            throw new RuntimeException(String.format("%s: rename segment %s failed", context, tempFile), e);
        }
    }

    protected abstract Integer currentSegmentVersion(Long segment);

    protected abstract void activateSegmentVersion(long segment, int version);

    protected void scheduleForDeletion(long segment, int version) {
        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                                                                  if (!removeSegment(segment, version)) {
                                                                      scheduleForDeletion(segment, version);
                                                                  }
                                                              },
                                                              storageProperties.getSecondaryCleanupDelay(),
                                                              TimeUnit.SECONDS);
    }

    protected abstract boolean removeSegment(long segment, int version);

    private <T> boolean contains(T[] values, T value) {
        for (T t : values) {
            if (t.equals(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, int maxSegmentsHint, long maxTokenHint) {
        long before = System.currentTimeMillis();
        try {
            return indexManager.getLastSequenceNumber(aggregateIdentifier, maxSegmentsHint, maxTokenHint);
        } finally {
            lastSequenceReadTimer.record(System.currentTimeMillis() - before, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber, long maxSequenceNumber) {

        SegmentIndexEntries lastEventPosition = indexManager.lastIndexEntries(aggregateId, maxSequenceNumber);
        if (lastEventPosition == null) {
            return Optional.empty();
        }

        return readSerializedEvent(minSequenceNumber, maxSequenceNumber, lastEventPosition);
    }

    private Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber,
                                                          SegmentIndexEntries lastEventPosition) {
        Optional<EventSource> eventSource = getEventSource(lastEventPosition.segment());
        if (eventSource.isPresent()) {
            try {
                List<Integer> positions = lastEventPosition.indexEntries().positions();
                for (int i = positions.size() - 1; i >= 0; i--) {
                    SerializedEvent event = eventSource.get().readEvent(positions.get(i));
                    if (event.getAggregateSequenceNumber() >= minSequenceNumber
                            && event.getAggregateSequenceNumber() < maxSequenceNumber) {
                        return Optional.of(event);
                    }
                }

                return Optional.empty();
            } finally {
                eventSource.get().close();
            }
        }

        if (next != null) {
            return next.readSerializedEvent(minSequenceNumber, maxSequenceNumber, lastEventPosition);
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
    public boolean keepOldVersions() {
        return storageProperties.isKeepOldVersions();
    }

    @Override
    public void init(boolean validate, long defaultFirstToken) {
        initSegments(Long.MAX_VALUE, defaultFirstToken);
        validate(validate ? storageProperties.getValidationSegments() : 2);
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
                try (EventIterator iterator = createEventIterator(es, segment, segment)) {
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

    public void initSegments(long maxValue, long defaultFirstToken) {
        initSegments(maxValue);
    }

    public void initSegments(long maxValue) {
    }

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
                     .orElseGet(() -> getTransactionsFromNext(segment, token, validating));
    }

    private TransactionIterator getTransactionsFromNext(long segment, long token, boolean validating) {
        if (next == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 String.format(
                                                         "%s: unable to read transactions for segment %d, requested token %d",
                                                         context,
                                                         segment,
                                                         token));
        }
        return next.getTransactions(segment, token, validating);
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
    public CloseableIterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return new TransactionWithTokenIterator(firstToken, limitToken);
    }

    protected Map<Long, Integer> prepareSegmentStore(long lastInitialized) {
        NavigableMap<Long, Integer> segments = new TreeMap<>(Comparator.reverseOrder());
        File events = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(events);
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());
        Arrays.stream(eventFiles)
              .map(FileUtils::process)
              .filter(segment -> segment.segment() < lastInitialized)
              .forEach(segment -> segments.compute(segment.segment(), (s, old) -> {
                  if (old == null) {
                      return segment.version();
                  }
                  return Math.max(old, segment.version());
              }));

        segments.forEach((segment, version) -> renameFileIfNecessary(segment));
        long firstValidIndex = segments.entrySet().stream().filter(entry -> indexManager.validIndex(new FileVersion(
                                               entry.getKey(),
                                               entry.getValue())))
                                       .map(Map.Entry::getKey)
                                       .findFirst().orElse(-1L);
        logger.debug("First valid index: {}", firstValidIndex);
        SortedSet<FileVersion> recreate = new TreeSet<>();
        recreate.addAll(segments.headMap(firstValidIndex).entrySet().stream()
                                .map(e -> new FileVersion(e.getKey(), e.getValue())).collect(Collectors.toSet()));
        recreate.forEach(this::recreateIndex);
        return segments;
    }

    protected abstract void recreateIndex(FileVersion segment);

    private int retrieveEventsForAnAggregate(FileVersion segment, List<Integer> indexEntries, long minSequenceNumber,
                                             long maxSequenceNumber,
                                             Consumer<SerializedEvent> onEvent, long maxResults, long minToken) {
        Optional<EventSource> buffer = getEventSource(segment);
        int processed = 0;

        if (buffer.isPresent()) {
            EventSource eventSource = buffer.get();
            for (int i = 0; i < indexEntries.size() && i < maxResults; i++) {
                SerializedEvent event = eventSource.readEvent(indexEntries.get(i));
                if (event.getAggregateSequenceNumber() >= minSequenceNumber
                        && event.getAggregateSequenceNumber() < maxSequenceNumber) {
                    onEvent.accept(event);
                }
                processed++;
            }
            eventSource.close();
        } else {
            if (next != null) {
                processed = next.retrieveEventsForAnAggregate(segment,
                                                              indexEntries,
                                                              minSequenceNumber,
                                                              maxSequenceNumber,
                                                              onEvent,
                                                              maxResults,
                                                              minToken);
            }
        }

        return processed;
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp) {
        Stream<String> filenames = Stream.concat(getSegments().stream()
                                                              .filter(s -> (s > lastSegmentBackedUp
                                                                      || currentSegmentVersion(s)
                                                                      > lastVersionBackedUp))
                                                              .map(s -> storageProperties.dataFile(context,
                                                                                                   new FileVersion(s,
                                                                                                                   currentSegmentVersion(
                                                                                                                           s)))
                                                                                         .getAbsolutePath()),
                                                 indexManager.getBackupFilenames(lastSegmentBackedUp,
                                                                                 lastVersionBackedUp));

        if (next == null) {
            return filenames;
        }
        return Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp));
    }

    protected void renameFileIfNecessary(long segment) {
        File dataFile = storageProperties.oldDataFile(context, segment);
        if (dataFile.exists()) {
            if (!dataFile.renameTo(storageProperties.dataFile(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(dataFile,
                                                                   storageProperties.dataFile(context, segment)));
            }
            File indexFile = storageProperties.oldIndex(context, segment);
            if (indexFile.exists() && !indexFile.renameTo(storageProperties.index(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(indexFile,
                                                                   storageProperties.index(context, segment)));
            }
            File bloomFile = storageProperties.oldBloomFilter(context, segment);
            if (bloomFile.exists() && !bloomFile.renameTo(storageProperties.bloomFilter(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(bloomFile,
                                                                   storageProperties.bloomFilter(context, segment)));
            }
        }
    }

    private String renameMessage(File from, File to) {
        return "Could not rename " + from.getAbsolutePath() + " to " + to.getAbsolutePath();
    }

    protected void recreateIndexFromIterator(FileVersion segment, EventIterator iterator) {
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
        indexManager.addToActiveSegment(segment.segment(), loadedEntries);
        indexManager.complete(segment);
    }

    /**
     * @param segment gets an EventSource for the segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    public abstract Optional<EventSource> getEventSource(FileVersion segment);

    //Retrieves event source from first available layer that is responsible for given segment
    private Optional<EventSource> eventSource(FileVersion segment) {
        Optional<EventSource> eventSource = getEventSource(segment);
        if (eventSource.isPresent()) {
            return eventSource;
        }
        if (next == null) {
            return Optional.empty();
        }
        return next.eventSource(segment);
    }

    /**
     * Get all segments
     *
     * @return descending set of segment ids
     */
    protected abstract SortedSet<Long> getSegments();

    @Override
    public byte transactionVersion() {
        return TRANSACTION_VERSION;
    }

    @Override
    public long nextToken() {
        return 0;
    }

    @Override
    public void validateTransaction(long token, List<SerializedEvent> eventList) {
        try (CloseableIterator<SerializedTransactionWithToken> transactionIterator = transactionIterator(token,
                                                                                                         token
                                                                                                                 + eventList
                                                                                                                 .size())) {
            if (transactionIterator.hasNext()) {
                SerializedTransactionWithToken transaction = transactionIterator.next();
                if (!transaction.getEvents().equals(eventList)) {
                    throw new EventStoreValidationException(String.format(
                            "%s: Replicated %s transaction %d does not match stored transaction",
                            context,
                            type.getEventType(),
                            token));
                }
            } else {
                throw new EventStoreValidationException(String.format("%s: Replicated %s transaction %d not found",
                                                                      context,
                                                                      type.getEventType(),
                                                                      token));
            }
        }
    }

    private class TransactionWithTokenIterator implements CloseableIterator<SerializedTransactionWithToken> {

        private final Long limitToken;
        private long currentToken;
        private long currentSegment;
        private TransactionIterator currentTransactionIterator;

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
            SerializedTransactionWithToken nextTransaction = currentTransactionIterator.next();
            currentToken += nextTransaction.getEventsCount();
            checkPointers();
            return nextTransaction;
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

        @Override
        public void close() {
            if (currentTransactionIterator != null) {
                currentTransactionIterator.close();
            }
        }
    }
}
