/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.EventStoreValidationException;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.localstorage.file.StorageProperties.TRANSFORMED_SUFFIX;
import static java.lang.String.format;

/**
 * File based implementation for the {@link EventStorageEngine}. Breaks up the event stores into segments, with each
 * segment stored in a separate file.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class FileEventStorageEngine implements EventStorageEngine {

    protected static final Logger logger = LoggerFactory.getLogger(FileEventStorageEngine.class);
    public static final int MAX_EVENTS_PER_BLOCK = Short.MAX_VALUE;
    private static final int PREFETCH_SEGMENT_FILES = 2;

    private final WritableFileStorageTier head;
    protected final Set<Runnable> closeListeners = new CopyOnWriteArraySet<>();

    private final String storagePath;
    private final EventTypeContext context;
    private final IndexManager indexManager;
    private final Supplier<StorageProperties> storagePropertiesSupplier;
    private final Timer lastSequenceReadTimer;
    private final DistributionSummary aggregateSegmentsCount;


    /**
     * @param context                   the context and the content type (events or snapshots)
     * @param indexManager              the index manager to use
     * @param eventTransformerFactory   the transformer factory
     * @param storagePropertiesSupplier supplies configuration of the storage engine
     * @param meterFactory              factory to create metrics meters
     * @param fileSystemMonitor
     */

    public FileEventStorageEngine(EventTypeContext context,
                                  IndexManager indexManager,
                                  EventTransformerFactory eventTransformerFactory,
                                  Supplier<StorageProperties> storagePropertiesSupplier,
                                  Supplier<StorageTier> completedSegmentsHandler,
                                  MeterFactory meterFactory,
                                  FileSystemMonitor fileSystemMonitor,
                                  String storagePath) {
        this.context = context;
        this.indexManager = indexManager;
        this.storagePropertiesSupplier = storagePropertiesSupplier;
        head = new WritableFileStorageTier(context,
                                           indexManager,
                                           storagePropertiesSupplier,
                                           completedSegmentsHandler,
                                           meterFactory,
                                           storagePath,
                                           eventTransformerFactory,
                                           fileSystemMonitor);
        this.storagePath = storagePath;
        Tags tags = Tags.of(MeterFactory.CONTEXT, context.getContext(), "type", context.getEventType().name());
        this.lastSequenceReadTimer = meterFactory.timer(BaseMetricName.AXON_LAST_SEQUENCE_READTIME, tags);
        this.aggregateSegmentsCount = meterFactory.distributionSummary
                                                          (BaseMetricName.AXON_AGGREGATE_SEGMENT_COUNT, tags);
    }

    @Override
    public void init(boolean validate, long defaultFirstIndex) {
        head.initSegments(Long.MAX_VALUE, defaultFirstIndex);
        validate(validate ? storagePropertiesSupplier.get().getValidationSegments() : 2);
    }

    public void validate(int maxSegments) {
        Set<Long> segments = head.allSegments().limit(maxSegments).collect(Collectors.toSet());
        List<ValidationResult> resultList = segments.parallelStream().map(this::validateSegment).collect(
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
                                                     format(
                                                             "Validation exception: segment %d ending at token, %d, next segment starts at token %d",
                                                             thisResult.getSegment(),
                                                             thisResult.getLastToken() - 1,
                                                             nextResult.getSegment()));
            }
        }
    }

    private ValidationResult validateSegment(long segment) {
        logger.debug("{}: Validating {} segment: {}", context.getContext(), context.getEventType(), segment);
        try (TransactionIterator iterator = getTransactions(segment, segment, true)) {
            SerializedTransactionWithToken last = null;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            return new ValidationResult(segment, last == null ? segment : last.getToken() + last.getEventsCount());
        } catch (Exception ex) {
            return new ValidationResult(segment, ex.getMessage());
        }
    }


    @Override
    public long getFirstCompletedSegment() {
        return head.getFirstCompletedSegment();
    }

    @Override
    public CompletableFuture<Long> store(List<Event> eventList, int segmentVersion) {
        return head.store(eventList, segmentVersion);
    }

    @Override
    public long getLastToken() {
        return head.getLastToken();
    }

    @Override
    public Flux<SerializedEvent> eventsPerAggregate(String aggregateId, long firstSequence,
                                                    long lastSequence, long minToken) {
        return Flux.defer(() -> {
                       logger.debug("Reading index entries for aggregate {} started.", aggregateId);

                       SortedMap<FileVersion, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                                         firstSequence,
                                                                                                         lastSequence,
                                                                                                         Long.MAX_VALUE,
                                                                                                         minToken);
                       logger.debug("Reading index entries for aggregate {} finished.", aggregateId);
                       aggregateSegmentsCount.record(positionInfos.size());

                       return Flux.fromIterable(positionInfos.entrySet());
                   }).flatMapSequential(e -> eventsForPositions(e.getKey(),
                                                                e.getValue(),
                                                                storagePropertiesSupplier.get().getEventsPerSegmentPrefetch()),
                                        PREFETCH_SEGMENT_FILES,
                                        storagePropertiesSupplier.get().getEventsPerSegmentPrefetch())
                   .skipUntil(se -> se.getAggregateSequenceNumber() >= firstSequence)
                   .takeWhile(se -> se.getAggregateSequenceNumber() < lastSequence)
                   .name("event_stream")
                   .tag("context", context.getContext())
                   .tag("stream", "aggregate_events")
                   .tag("origin", "event_sources")
                   .metrics();
    }

    private Flux<SerializedEvent> eventsForPositions(FileVersion segment, IndexEntries indexEntries, int prefetch) {
        return new EventSourceFlux(indexEntries,
                                   () -> head.eventSource(segment),
                                   segment.segment(),
                                   prefetch).get()
                                            .name("event_stream")
                                            .tag("context", context.getContext())
                                            .tag("stream",
                                                 "aggregate_events")
                                            .tag("origin",
                                                 "event_source")
                                            .metrics();
    }

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
                                                                                      Long.MAX_VALUE));
    }

    @Override
    public EventTypeContext getType() {
        return context;
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp, boolean includeActive) {
        return head.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp, includeActive);
    }

    @Override
    public byte transactionVersion() {
        return AbstractFileStorageTier.TRANSACTION_VERSION;
    }

    @Override
    public long nextToken() {
        return head.nextToken();
    }

    @Override
    public void close(boolean deleteData) {
        head.close(deleteData);
        closeListeners.forEach(Runnable::run);
    }

    @Override
    public Registration registerCloseListener(Runnable listener) {
        closeListeners.add(listener);
        return () -> closeListeners.remove(listener);
    }

    @Override
    public Flux<Long> transformContents(int transformationVersion, Flux<EventWithToken> transformedEvents) {
        // TODO: 7/26/22 revisit this approach!!!
        return doTransformContents(transformationVersion, transformedEvents);
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
                                                       maxResults);
            if (maxResults <= 0) {
                return;
            }
        }
    }

    private int retrieveEventsForAnAggregate(FileVersion segment, List<Integer> indexEntries, long minSequenceNumber,
                                             long maxSequenceNumber,
                                             Consumer<SerializedEvent> onEvent, long maxResults) {
        Optional<EventSource> buffer = head.eventSource(segment);
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
        }

        return processed;
    }


    @Override
    public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {
        boolean snapshot = !context.isEvent();
        head.allSegments()
            .filter(s -> s <= queryOptions.getMaxToken())
            .map(s -> head.eventSource(s).orElse(null))
            .filter(Objects::nonNull)
            .anyMatch(eventSource -> {
                boolean done = false;
                long minTimestampInSegment = Long.MAX_VALUE;
                EventInformation eventWithToken;
                EventIterator iterator = eventSource.createEventIterator();
                while (!done && iterator.hasNext()) {
                    eventWithToken = iterator.next();
                    minTimestampInSegment = Math.min(minTimestampInSegment,
                                                     eventWithToken.getEvent().getTimestamp());
                    if (eventWithToken.getToken() > queryOptions.getMaxToken()) {
                        done = true;
                    }

                    if (!done && eventWithToken.getToken() >= queryOptions.getMinToken()
                            && eventWithToken.getEvent().getTimestamp()
                            >= queryOptions.getMinTimestamp()
                            && !consumer.test(eventWithToken.asEventWithToken(snapshot))) {
                        done = true;
                    }
                }
                if (queryOptions.getMinToken() > eventSource.segment()
                        || minTimestampInSegment < queryOptions
                        .getMinTimestamp()) {
                    done = true;
                }
                iterator.close();
                eventSource.close();

                return done;
            });
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint[] hints) {
        return getLastSequenceNumber(aggregateIdentifier, recentOnly(hints) ?
                storagePropertiesSupplier.get().segmentsForSequenceNumberCheck() : Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    private Flux<Long> doTransformContents(int transformationVersion, Flux<EventWithToken> transformedEvents) {
        return Flux.usingWhen(Mono.just(new TransformationResources(this::getSegmentFor,
                                                                    storagePropertiesSupplier,
                                                                    transformationVersion,
                                                                    indexManager,
                                                                    this::transactionsForTransformation,
                                                                    storagePath)),
                              resources -> transformedEvents.concatMap(event -> resources.transform(event)
                                                                                         .thenReturn(1L)),
                              currentSegmentTransformer -> currentSegmentTransformer.completeCurrentSegment()
                                                                                    .then(activateTransformation()),
                              TransformationResources::rollback,
                              TransformationResources::cancel);
    }

    private Mono<Void> activateTransformation() {
        return transformedSegmentVersions()
                .flatMap(this::activateSegment)
                .then();
    }

    private Flux<FileVersion> transformedSegmentVersions() {
        String transformedEventsSuffix = storagePropertiesSupplier.get().getEventsSuffix() + TRANSFORMED_SUFFIX;
        return fileVersions(transformedEventsSuffix);
    }

    private Flux<FileVersion> fileVersions(String suffix) {
        return Flux.fromArray(FileUtils.getFilesWithSuffix(new File(storagePath), suffix))
                   .map(FileUtils::process);
    }

    private Mono<Void> activateSegment(FileVersion fileVersion) {
        return renameTransformedSegmentIfNeeded(fileVersion)
                .then(indexManager.activateVersion(fileVersion))
                .then(Mono.fromRunnable(() -> head.activateSegmentVersion(fileVersion.segment(),
                                                                          fileVersion.segmentVersion())));
    }

    private Mono<Void> renameTransformedSegmentIfNeeded(FileVersion fileVersion) {
        return Mono.fromSupplier(() -> storagePropertiesSupplier.get().dataFile(storagePath, fileVersion))
                   .filter(dataFile -> !dataFile.exists())
                   .flatMap(dataFile -> Mono.fromSupplier(() -> transformedDataFile(fileVersion))
                                            .filter(File::exists)
                                            .switchIfEmpty(Mono.error(new RuntimeException("File does not exist.")))
                                            .flatMap(tempFile -> FileUtils.rename(tempFile, dataFile)));
    }

    private File transformedDataFile(FileVersion segment) {
        return storagePropertiesSupplier.get().transformedDataFile(storagePath, segment);
    }

    @Override
    public Mono<Void> deleteOldVersions() {
        return fileVersions(storagePropertiesSupplier.get().getEventsSuffix())
                .filter(fileVersion -> head.currentSegmentVersion(fileVersion.segment())
                        != fileVersion.segmentVersion())
                .flatMapSequential(this::delete)
                .then();
    }

    private Mono<Void> delete(FileVersion fileVersion) {
        return Mono.fromRunnable(() -> {
                                     if (!head.removeSegment(fileVersion.segment(), fileVersion.segmentVersion())) {
                                         throw new MessagingPlatformException(ErrorCode.OTHER,
                                                                              context + ": Cannot remove segment "
                                                                                      + fileVersion);
                                     }
                                 }
                   ).retry(3)
                   .then();
    }

    @Override
    public void validateTransaction(long token, List<Event> eventList) {
        try (CloseableIterator<SerializedTransactionWithToken> transactionIterator =
                     transactionIterator(token, token + eventList.size())) {
            if (transactionIterator.hasNext()) {
                SerializedTransactionWithToken transaction = transactionIterator.next();
                if (!equals(transaction.getEvents(), eventList)) {
                    throw new EventStoreValidationException(format(
                            "%s: Replicated %s transaction %d does not match stored transaction",
                            context.getContext(),
                            context.getEventType(),
                            token));
                }
            } else {
                throw new EventStoreValidationException(format("%s: Replicated %s transaction %d not found",
                                                               context.getContext(),
                                                               context.getEventType(),
                                                               token));
            }
        }
    }

    private boolean equals(List<SerializedEvent> storedEvents, List<Event> eventList) {
        for (int i = 0; i < storedEvents.size(); i++) {
            if (!storedEvents.get(i).asEvent().equals(eventList.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean recentOnly(SearchHint[] values) {
        for (SearchHint t : values) {
            if (t.equals(SearchHint.RECENT_ONLY)) {
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

    @Override
    public CloseableIterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return new TransactionWithTokenIterator(firstToken, limitToken);
    }

    private TransactionIterator createTransactionIterator(EventSource eventSource, long token,
                                                          boolean validating) {
        return eventSource.createTransactionIterator(token, validating);
    }


    private TransactionIterator transactionsForTransformation(long segment) {
        head.forceNextSegmentIfNeeded(segment);
        return getTransactions(segment, segment, false);
    }

    private TransactionIterator getTransactions(long segment, long token, boolean validating) {
        Optional<EventSource> reader = head.eventSource(segment);
        return reader.map(r -> createTransactionIterator(r, token, validating))
                     .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                       format(
                                                                               "%s: unable to read transactions for segment %d, requested token %d",
                                                                               context,
                                                                               segment,
                                                                               token)));
    }

    private EventIterator getEvents(long segment, long token) {
        Optional<EventSource> reader = head.eventSource(segment);
        return reader.map(eventSource -> eventSource.createEventIterator(token))
                     .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                       format("%s: token %d before start of event store",
                                                                              context,
                                                                              token)));
    }


    private Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber,
                                                          SegmentIndexEntries lastEventPosition) {
        return head.eventSource(lastEventPosition.fileVersion())
                   .map(e -> {
                       try (e) {
                           return e.readLastInRange(minSequenceNumber,
                                                    maxSequenceNumber,
                                                    lastEventPosition.indexEntries().positions());
                       }
                   });
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {

        return new CloseableIterator<>() {

            long nextToken = start;
            EventIterator eventIterator;
            final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public void close() {
                closed.set(true);
                if (eventIterator != null) {
                    eventIterator.close();
                }
            }

            @Override
            public boolean hasNext() {
                if (closed.get()) {
                    throw new IllegalStateException("Iterator is closed");
                }
                return nextToken <= getLastToken();
            }

            @Override
            public SerializedEventWithToken next() {
                if (closed.get()) {
                    throw new IllegalStateException("Iterator is closed");
                }
                if (eventIterator == null) {
                    eventIterator = getEvents(getSegmentFor(nextToken), nextToken);
                }
                SerializedEventWithToken event = null;
                if (eventIterator.hasNext()) {
                    event = eventIterator.next().getSerializedEventWithToken();
                } else {
                    eventIterator.close();
                    eventIterator = getEvents(getSegmentFor(nextToken), nextToken);
                    if (eventIterator.hasNext()) {
                        event = eventIterator.next().getSerializedEventWithToken();
                    }
                }
                if (event != null) {
                    nextToken = event.getToken() + 1;
                    return event;
                }
                throw new NoSuchElementException("No event for token " + nextToken);
            }
        };
    }

    public int activeSegmentCount() {
        return head.activeSegmentCount();
    }

    public void handover(Segment segment, Runnable callback) {
        head.handover(segment, callback);
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

        private TransactionIterator getTransactions(long segment, long token) {
            return FileEventStorageEngine.this.getTransactions(segment, token, false);
        }
    }

    @Override
    public long getFirstToken() {
        return head.allSegments().reduce((acc, id) -> id).orElse(-1L);
    }

    @Override
    public long getTokenAt(long instant) {
        return head.allSegments()
                   .map(s -> head.eventSource(s).orElse(null))
                   .filter(Objects::nonNull)
                   .map(es -> {
                       try (EventIterator iterator = es.createEventIterator(es.segment())) {
                           return iterator.getTokenAt(instant);
                       }
                   })
                   .filter(Objects::nonNull)
                   .findFirst()
                   .orElseGet(this::getFirstToken);
    }

    private long getSegmentFor(long token) {
        return head.allSegments()
                   .filter(segment -> segment <= token)
                   .findFirst()
                   .orElse(-1L);
    }
}
