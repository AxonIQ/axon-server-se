/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.StorageCallback;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.localstorage.file.FileUtils.name;
import static io.axoniq.axonserver.localstorage.file.UpgradeUtils.fixInvalidFileName;
import static java.lang.String.format;

/**
 * Manages the writable segments of the event store. Once the segment is completed this class hands the segment over to
 * the next segment based event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class PrimaryEventStore implements EventStorageEngine {

    protected static final Logger logger = LoggerFactory.getLogger(PrimaryEventStore.class);
    public static final int MAX_EVENTS_PER_BLOCK = Short.MAX_VALUE;
    public static final byte TRANSACTION_VERSION = 2;
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
    protected static final int MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK = 10;
    private static final int PREFETCH_SEGMENT_FILES = 2;
    private final DistributionSummary aggregateSegmentsCount;

    private final String context;
    private final IndexManager indexManager;
    protected final EventTransformerFactory eventTransformerFactory;
    protected final Synchronizer synchronizer;
    protected final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    protected final AtomicLong lastToken = new AtomicLong(-1);
    //
    protected final ConcurrentNavigableMap<Long, ByteBufferEventSource> readBuffers = new ConcurrentSkipListMap<>();
    protected EventTransformer eventTransformer;
    @org.jetbrains.annotations.NotNull private final Supplier<StorageProperties> storagePropertiesSupplier;
    private final SegmentGroup next;
    protected final FileSystemMonitor fileSystemMonitor;

    protected final EventTypeContext type;
    protected final Set<Runnable> closeListeners = new CopyOnWriteArraySet<>();

    private final Timer lastSequenceReadTimer;


    /**
     * @param context                   the context and the content type (events or snapshots)
     * @param indexManager              the index manager to use
     * @param eventTransformerFactory   the transformer factory
     * @param storagePropertiesSupplier supplies configuration of the storage engine
     * @param meterFactory              factory to create metrics meters
     * @param fileSystemMonitor
     */
    public PrimaryEventStore(EventTypeContext context,
                             IndexManager indexManager,
                             EventTransformerFactory eventTransformerFactory,
                             Supplier<StorageProperties> storagePropertiesSupplier,
                             SegmentBasedEventStore completedSegmentsHandler,
                             MeterFactory meterFactory,
                             FileSystemMonitor fileSystemMonitor) {
        this(context,
             indexManager,
             eventTransformerFactory,
             storagePropertiesSupplier,
             completedSegmentsHandler,
             meterFactory,
             fileSystemMonitor,
             Short.MAX_VALUE);
    }

    public PrimaryEventStore(EventTypeContext context,
                             IndexManager indexManager,
                             EventTransformerFactory eventTransformerFactory,
                             Supplier<StorageProperties> storagePropertiesSupplier,
                             SegmentBasedEventStore completedSegmentsHandler,
                             MeterFactory meterFactory,
                             FileSystemMonitor fileSystemMonitor,
                             short maxEventsPerTransaction) {
        this.context = context.getContext();
        this.type = context;
        this.indexManager = indexManager;
        this.eventTransformerFactory = eventTransformerFactory;
        this.storagePropertiesSupplier = storagePropertiesSupplier;
        this.next = completedSegmentsHandler;
        this.fileSystemMonitor = fileSystemMonitor;
        Tags tags = Tags.of(MeterFactory.CONTEXT, context.getContext(), "type", context.getEventType().name());
        this.lastSequenceReadTimer = meterFactory.timer(BaseMetricName.AXON_LAST_SEQUENCE_READTIME, tags);
        this.aggregateSegmentsCount = meterFactory.distributionSummary
                                                          (BaseMetricName.AXON_AGGREGATE_SEGMENT_COUNT, tags);

        synchronizer = new Synchronizer(context, storagePropertiesSupplier.get(), this::completeSegment);
    }

    @Override
    public void init(boolean validate, long defaultFirstToken) {
        initSegments(Long.MAX_VALUE, defaultFirstToken);
        validate(validate ? storagePropertiesSupplier.get().getValidationSegments() : 2);
    }

    public void initSegments(long lastInitialized, long defaultFirstIndex) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File storageDir = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(storageDir);
        indexManager.init();
        eventTransformer = eventTransformerFactory.get(storageProperties.getFlags());
        initLatestSegment(lastInitialized, Long.MAX_VALUE, storageDir, defaultFirstIndex, storageProperties);

        fileSystemMonitor.registerPath(storeName(), storageDir.toPath());
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
                                                     format(
                                                             "Validation exception: segment %d ending at token, %d, next segment starts at token %d",
                                                             thisResult.getSegment(),
                                                             thisResult.getLastToken() - 1,
                                                             nextResult.getSegment()));
            }
        }
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

    private Stream<Long> getAllSegments() {
        if (next == null) {
            return getSegments().stream();
        }
        return Stream.concat(getSegments().stream(), next.getAllSegments()).distinct();
    }


    private void initLatestSegment(long lastInitialized, long nextToken, File storageDir, long defaultFirstIndex,
                                   StorageProperties storageProperties) {
        long first = getFirstFile(lastInitialized, storageDir, defaultFirstIndex, storageProperties);
        fixInvalidFileName(first, storageProperties, context);
        first = firstSegmentIfLatestCompleted(first, storageProperties);
        if (next != null) {
            next.initSegments(first);
        }
        WritableEventSource buffer = getOrOpenDatafile(first, storageProperties.getSegmentSize(), false);
        indexManager.remove(first);
        long sequence = first;
        Map<String, List<IndexEntry>> loadedEntries = new HashMap<>();
        try (EventByteBufferIterator iterator = new EventByteBufferIterator(buffer, first, first)) {
            while (sequence < nextToken && iterator.hasNext()) {
                EventInformation event = iterator.next();
                if (event.isDomainEvent()) {
                    IndexEntry indexEntry = new IndexEntry(
                            event.getEvent().getAggregateSequenceNumber(),
                            event.getPosition(),
                            sequence
                    );
                    loadedEntries.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
                                                  aggregateId -> new LinkedList<>())
                                 .add(indexEntry);
                }
                sequence++;
            }
            List<EventInformation> pendingEvents = iterator.pendingEvents();
            if (!pendingEvents.isEmpty()) {
                logger.warn(
                        "Failed to position to transaction {}, {} events left in transaction, moving to end of transaction",
                        nextToken,
                        pendingEvents.size());
                for (EventInformation event : pendingEvents) {
                    if (event.isDomainEvent()) {
                        IndexEntry indexEntry = new IndexEntry(
                                event.getEvent().getAggregateSequenceNumber(),
                                event.getPosition(),
                                sequence
                        );
                        loadedEntries.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
                                                      aggregateId -> new LinkedList<>())
                                     .add(indexEntry);
                    }
                    sequence++;
                }
            }
            lastToken.set(sequence - 1);
        }

        indexManager.addToActiveSegment(first, loadedEntries);

        buffer.putInt(buffer.position(), 0);
        WritePosition writePosition = new WritePosition(sequence, buffer.position(), buffer, first, 0);
        writePositionRef.set(writePosition);
        synchronizer.init(writePosition);
    }

    public int activeSegmentCount() {
        return readBuffers.size();
    }

    private long firstSegmentIfLatestCompleted(long latestSegment, StorageProperties storageProperties) {
        if (!indexManager.validIndex(latestSegment)) {
            return latestSegment;
        }
        WritableEventSource buffer = getOrOpenDatafile(latestSegment, storageProperties.getSegmentSize(), false);
        long token = latestSegment;
        try (EventByteBufferIterator iterator = new EventByteBufferIterator(buffer, latestSegment, latestSegment)) {
            while (iterator.hasNext()) {
                iterator.next();
                token++;
            }
        } finally {
            readBuffers.remove(latestSegment);
            buffer.close();
        }
        return token;
    }

    private long getFirstFile(long lastInitialized, File events, long defaultFirstIndex,
                              StorageProperties storageProperties) {
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());

        return Arrays.stream(eventFiles)
                     .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
                     .filter(segment -> segment < lastInitialized)
                     .max(Long::compareTo)
                     .orElse(defaultFirstIndex);
    }

    private FilePreparedTransaction prepareTransaction(List<Event> origEventList) {
        List<ProcessedEvent> eventList = origEventList.stream().map(s -> new WrappedEvent(s, eventTransformer)).collect(
                Collectors.toList());
        int eventSize = eventBlockSize(eventList);
        WritePosition writePosition = claim(eventSize, eventList.size());
        return new FilePreparedTransaction(writePosition, eventSize, eventList);
    }

    /**
     * Stores a list of events. Completable future completes when these events and all previous events are written.
     *
     * @param events the events to store
     * @return completable future with the token of the first event
     */
    @Override
    public CompletableFuture<Long> store(List<Event> events) {

        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            Map<String, List<IndexEntry>> indexEntries = new HashMap<>();
            FilePreparedTransaction preparedTransaction = prepareTransaction(events);
            WritePosition writePosition = preparedTransaction.getWritePosition();

            synchronizer.register(writePosition, new StorageCallback() {
                private final AtomicBoolean running = new AtomicBoolean();

                @Override
                public boolean complete(long firstToken) {
                    if (running.compareAndSet(false, true)) {
                        indexManager.addToActiveSegment(writePosition.segment, indexEntries);
                        completableFuture.complete(firstToken);
                        lastToken.set(firstToken + preparedTransaction.getEventList().size() - 1);
                        return true;
                    }
                    return false;
                }

                @Override
                public void error(Throwable cause) {
                    completableFuture.completeExceptionally(cause);
                }
            });
            write(writePosition, preparedTransaction.getEventList(), indexEntries);
            synchronizer.notifyWritePositions();
        } catch (RuntimeException cause) {
            completableFuture.completeExceptionally(cause);
        }

        return completableFuture;
    }

    @Override
    public byte transactionVersion() {
        return TRANSACTION_VERSION;
    }

    @Override
    public void close(boolean deleteData) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File storageDir = new File(storageProperties.getStorage(context));
        fileSystemMonitor.unregisterPath(storeName());

        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> {
            source.clean(0);
            if (deleteData) {
                removeSegment(s, storageProperties);
            }
        });

        if (next != null) {
            next.close(deleteData);
        }

        indexManager.cleanup(deleteData);
        if (deleteData) {
            storageDir.delete();
        }
        closeListeners.forEach(Runnable::run);
    }

    @Override
    public Registration registerCloseListener(Runnable listener) {
        closeListeners.add(listener);
        return () -> closeListeners.remove(listener);
    }

    @Override
    public EventTypeContext getType() {
        return type;
    }


    private NavigableSet<Long> getSegments() {
        return readBuffers.descendingKeySet();
    }

    private Optional<EventSource> getEventSource(long segment) {
        if (readBuffers.containsKey(segment)) {
            return Optional.of(readBuffers.get(segment).duplicate());
        }
        return Optional.empty();
    }

    @Override
    public long getLastToken() {
        return lastToken.get();
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp, boolean includeActive) {
        if (includeActive) {
            StorageProperties storageProperties = storagePropertiesSupplier.get();
            Stream<String> filenames = getSegments().stream()
                                                    .map(s -> name(storageProperties.dataFile(context, s)));
            return next != null ?
                    Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp)) :
                    filenames;
        }
        return next != null ? next.getBackupFilenames(lastSegmentBackedUp) : Stream.empty();
    }

    @Override
    public long getFirstToken() {
        return 0;
    }

    @Override
    public long getTokenAt(long instant) {
        return 0;
    }

    @Override
    public long getFirstCompletedSegment() {
        return next == null ? -1 : next.getFirstCompletedSegment();
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint[] hints) {
        return getLastSequenceNumber(aggregateIdentifier, contains(hints, SearchHint.RECENT_ONLY) ?
                MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK : Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    private <T> boolean contains(T[] values, T value) {
        for (T t : values) {
            if (t.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private long getSegmentFor(long token) {
        return getSegments().stream()
                            .filter(segment -> segment <= token)
                            .findFirst()
                            .orElse(next == null ? -1 : next.getSegmentFor(token));
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
    public Flux<SerializedEvent> eventsPerAggregate(String aggregateId,
                                                    long firstSequence,
                                                    long lastSequence,
                                                    long minToken) {
        return Flux.defer(() -> {
                       logger.debug("Reading index entries for aggregate {} started.", aggregateId);

                       SortedMap<Long, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
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
                   .skipUntil(se -> se.getAggregateSequenceNumber()
                           >= firstSequence) //todo for safe guard, remove in 4.6
                   .takeWhile(se -> se.getAggregateSequenceNumber() < lastSequence)
                   .name("event_stream")
                   .tag("context", context)
                   .tag("stream", "aggregate_events")
                   .tag("origin", "event_sources")
                   .metrics();
    }

    public Flux<SerializedEvent> eventsForPositions(long segment, IndexEntries indexEntries, int prefetch) {
        return new EventSourceFlux(indexEntries,
                                   () -> eventSource(segment),
                                   segment,
                                   prefetch).get()
                                            .name("event_stream")
                                            .tag("context", context)
                                            .tag("stream",
                                                 "aggregate_events")
                                            .tag("origin",
                                                 "event_source")
                                            .metrics();
    }

    private Optional<EventSource> eventSource(long segment) {
        return Optional.ofNullable(getEventSource(segment).orElseGet(() -> {
            if (next != null) {
                return next.eventSource(segment).orElse(null);
            }
            return null;
        }));
    }


    @Override
    public void processEventsPerAggregate(String aggregateId, long firstSequenceNumber, long lastSequenceNumber,
                                          long minToken, Consumer<SerializedEvent> eventConsumer) {
        SortedMap<Long, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
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
        SortedMap<Long, IndexEntries> positionInfos = indexManager.lookupAggregate(aggregateId,
                                                                                   firstSequenceNumber,
                                                                                   maxSequenceNumber,
                                                                                   maxResults,
                                                                                   0);

        List<Long> segmentsContainingAggregate = new ArrayList<>(positionInfos.keySet());
        Collections.reverse(segmentsContainingAggregate);
        for (Long segmentContainingAggregate : segmentsContainingAggregate) {
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
    public CloseableIterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return new TransactionWithTokenIterator(firstToken, limitToken);
    }

    @Override
    public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {
        List<Long> segments = getAllSegments().collect(Collectors.toList());
        for (long segment : segments) {
            if (segment <= queryOptions.getMaxToken()) {
                Optional<EventSource> eventSource = eventSource(segment);
                AtomicBoolean done = new AtomicBoolean();
                boolean snapshot = EventType.SNAPSHOT.equals(type.getEventType());
                eventSource.ifPresent(e -> {
                    long minTimestampInSegment = Long.MAX_VALUE;
                    EventInformation eventWithToken;
                    EventIterator iterator = e.createEventIterator(segment, segment);
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
                });
                if (done.get()) {
                    return;
                }
            }
        }
    }

    private TransactionIterator getTransactions(long segment, long token) {
        return getTransactions(segment, token, false);
    }

    private TransactionIterator getTransactions(long segment, long token, boolean validating) {
        Optional<EventSource> reader = eventSource(segment);
        return reader.map(r -> r.createTransactionIterator(segment, token, validating))
                     .orElseThrow(() -> new RuntimeException("Cannot find segment: " + segment));
    }


    public Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber,
                                                         SegmentIndexEntries lastEventPosition) {
        return Optional.ofNullable(eventSource(lastEventPosition.segment())
                                           .map(source -> {
                                               try {
                                                   return source.readEvent(lastEventPosition.indexEntries().positions(),
                                                                           minSequenceNumber, maxSequenceNumber);
                                               } finally {
                                                   source.close();
                                               }
                                           })
                                           .orElseThrow(() -> new RuntimeException(("Cannot find segment: "
                                                   + lastEventPosition.segment()))));
    }

    public int retrieveEventsForAnAggregate(long segment, List<Integer> indexEntries, long minSequenceNumber,
                                            long maxSequenceNumber,
                                            Consumer<SerializedEvent> onEvent, long maxResults, long minToken) {
        Optional<EventSource> buffer = eventSource(segment);
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
            throw new RuntimeException("Cannot find segment: " + segment);
        }

        return processed;
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
                            context,
                            type.getEventType(),
                            token));
                }
            } else {
                throw new EventStoreValidationException(format("%s: Replicated %s transaction %d not found",
                                                               context,
                                                               type.getEventType(),
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


    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {

        return new CloseableIterator<SerializedEventWithToken>() {

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

    private EventIterator getEvents(long segment, long token) {
        Optional<EventSource> reader = eventSource(segment);
        return reader.map(eventSource -> eventSource.createEventIterator(segment, token))
                     .orElseThrow(() -> new RuntimeException("Cannot find segment: " + segment));
    }


    private void removeSegment(long segment, StorageProperties storageProperties) {
        indexManager.remove(segment);
        ByteBufferEventSource eventSource = readBuffers.remove(segment);
        if (eventSource != null) {
            eventSource.clean(0);
        }
        FileUtils.delete(storageProperties.dataFile(context, segment));
    }

    protected void completeSegment(WritePosition writePosition) {
        indexManager.complete(writePosition.segment);
        if (next != null) {
            next.handover(new Segment() {
                @Override
                public Supplier<FileChannel> contentProvider() {
                    return () -> fileChannel(writePosition.segment);
                }

                @Override
                public long id() {
                    return writePosition.segment;
                }
            }, () -> {
                ByteBufferEventSource source = readBuffers.remove(writePosition.segment);
                logger.debug("Handed over {}, remaining segments: {}",
                             writePosition.segment,
                             getSegments());
                if (source != null) {
                    source.clean(storagePropertiesSupplier.get()
                                                          .getPrimaryCleanupDelay());
                }
            });
        }
    }

    private FileChannel fileChannel(Long s) {
        try {
            return FileChannel.open(storagePropertiesSupplier.get().dataFile(context, s).toPath(),
                                    StandardOpenOption.READ);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void write(WritePosition writePosition, List<ProcessedEvent> eventList,
                       Map<String, List<IndexEntry>> indexEntries) {
        ByteBufferEventSource source = writePosition.buffer.duplicate();
        ByteBuffer writeBuffer = source.getBuffer();
        writeBuffer.position(writePosition.position);
        int count = eventList.size();
        int from = 0;
        int to = Math.min(count, from + MAX_EVENTS_PER_BLOCK);
        int firstSize = writeBlock(writeBuffer, eventList, 0, to, indexEntries, writePosition.sequence);
        while (to < count) {
            from = to;
            to = Math.min(count, from + MAX_EVENTS_PER_BLOCK);
            int positionBefore = writeBuffer.position();
            int blockSize = writeBlock(writeBuffer, eventList, from, to, indexEntries, writePosition.sequence + from);
            int positionAfter = writeBuffer.position();
            writeBuffer.putInt(positionBefore, blockSize);
            writeBuffer.position(positionAfter);
        }
        writeBuffer.putInt(writePosition.position, firstSize);
        source.close();
    }

    private int writeBlock(ByteBuffer writeBuffer, List<ProcessedEvent> eventList, int from, int to,
                           Map<String, List<IndexEntry>> indexEntries, long token) {
        writeBuffer.putInt(0);
        writeBuffer.put(TRANSACTION_VERSION);
        writeBuffer.putShort((short) (to - from));
        Checksum checksum = new Checksum();
        int eventsPosition = writeBuffer.position();
        int eventsSize = 0;
        for (int i = from; i < to; i++) {
            ProcessedEvent event = eventList.get(i);
            int position = writeBuffer.position();
            writeBuffer.putInt(event.getSerializedSize());
            writeBuffer.put(event.toByteArray());
            if (event.isDomainEvent()) {
                indexEntries.computeIfAbsent(event.getAggregateIdentifier(),
                                             k -> new ArrayList<>())
                            .add(new IndexEntry(event.getAggregateSequenceNumber(), position, token));
            }
            eventsSize += event.getSerializedSize() + 4;
            token++;
        }

        writeBuffer.putInt(checksum.update(writeBuffer, eventsPosition, writeBuffer.position() - eventsPosition).get());
        return eventsSize;
    }

    private WritePosition claim(int eventBlockSize, int nrOfEvents) {
        int blocks = (int) Math.ceil(nrOfEvents / (double) MAX_EVENTS_PER_BLOCK);
        int totalSize = eventBlockSize + blocks * (HEADER_BYTES + TX_CHECKSUM_BYTES);
        if (totalSize > MAX_TRANSACTION_SIZE || eventBlockSize <= 0) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 String.format("Illegal transaction size: %d", eventBlockSize));
        }
        WritePosition writePosition;
        do {
            writePosition = writePositionRef.getAndUpdate(prev -> prev.incrementedWith(nrOfEvents, totalSize));

            if (writePosition.isOverflow(totalSize)) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", context, writePosition.sequence);

                writePosition.buffer.putInt(writePosition.position, -1);

                WritableEventSource buffer = getOrOpenDatafile(writePosition.sequence,
                                                               totalSize + FILE_HEADER_SIZE + FILE_FOOTER_SIZE,
                                                               true);
                writePositionRef.set(writePosition.reset(buffer));
            }
        } while (!writePosition.isWritable(totalSize));

        return writePosition;
    }

    @Override
    public long nextToken() {
        return writePositionRef.get().sequence;
    }

    protected WritableEventSource getOrOpenDatafile(long segment, int minSize, boolean canReplaceFile) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File file = storageProperties.dataFile(context, segment);
        int size = Math.max(storageProperties.getSegmentSize(), minSize);
        if (file.exists()) {
            if (canReplaceFile && file.length() < minSize) {
                ByteBufferEventSource s = readBuffers.remove(segment);
                if (s != null) {
                    s.clean(0);
                }
                FileUtils.delete(file);
            } else {
                size = (int) file.length();
            }
        }
        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            logger.info("Opening file {}", file);
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buffer.put(VERSION);
            buffer.putInt(storageProperties.getFlags());
            WritableEventSource writableEventSource = new WritableEventSource(file.getAbsolutePath(),
                                                                              buffer,
                                                                              eventTransformer,
                                                                              storageProperties.isCleanRequired());
            readBuffers.put(segment, writableEventSource);
            return writableEventSource;
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                 "Failed to open segment: " + segment,
                                                 ioException);
        }
    }

    private int eventBlockSize(List<ProcessedEvent> eventList) {
        long size = 0;
        for (ProcessedEvent event : eventList) {
            size += 4 + event.getSerializedSize();
        }
        if (size > Integer.MAX_VALUE) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Transaction size exceeds maximum size");
        }
        return (int) size;
    }

    private String storeName() {
        return context + "-" + type.getEventType().name().toLowerCase();
    }
}
