package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.grpc.EventWithToken;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.internal.grpc.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventInformation;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.StorageCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public abstract class SegmentBasedEventStore implements EventStore {
    protected static final Logger logger = LoggerFactory.getLogger(SegmentBasedEventStore.class);

    static final int TRANSACTION_LENGTH_BYTES = 4;
    static final int EVENT_LENGTH_BYTES = 4;
    static final int NUMBER_OF_EVENTS_BYTES = 2;
    static final int VERSION_BYTES = 1;
    static final int FILE_TX_COUNT_OFFSET = 1;
    static final int FILE_TX_COUNT_BYTES = 4;
    static final int TX_CHECKSUM_BYTES = 4;
    static final int HEADER_BYTES = TRANSACTION_LENGTH_BYTES + VERSION_BYTES + NUMBER_OF_EVENTS_BYTES;
    static final byte VERSION = 1;
    protected final String context;
    protected final IndexManager indexManager;
    protected final StorageProperties storageProperties;
    protected volatile SegmentBasedEventStore next;
    private final EventTypeContext type;

    public SegmentBasedEventStore(EventTypeContext eventTypeContext, IndexManager indexManager, StorageProperties storageProperties) {
        this.type = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storageProperties = storageProperties;
    }

    protected boolean foundFirstSequenceNumber(SortedSet<PositionInfo> positionInfos, long minSequenceNumber) {
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

    protected Optional<PositionInfo> getLastPosition(String aggregateIdentifier, int maxSegments) {
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
    public void streamByAggregateId(String aggregateId, long firstSequenceNumber, Consumer<Event> eventConsumer) {
        SortedMap<Long, SortedSet<PositionInfo>> positionInfos = getPositionInfos(aggregateId, firstSequenceNumber);
        boolean delegate = true;
        if( ! positionInfos.isEmpty()) {
            SortedSet<PositionInfo> first = positionInfos.get(positionInfos.firstKey());
            if( foundFirstSequenceNumber(first, firstSequenceNumber)) delegate = false;
        }

        if( delegate && next != null) {
            next.streamByAggregateId(aggregateId, firstSequenceNumber, eventConsumer);
        }

        positionInfos.keySet()
                     .forEach(segment -> retrieveEventsForAnAggregate(segment,
                                                                          positionInfos.get(segment),
                                                                          eventConsumer));

    }

    @Override
    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        for( long segment: getSegments() ) {
            Optional<EventSource> eventSource = getEventSource(segment);
            eventSource.ifPresent(e -> {
                long minTimestampInSegment = Long.MAX_VALUE;
                EventInformation eventWithToken;
                Iterator<EventInformation> iterator = createEventIterator(e,segment,segment);
                while (iterator.hasNext()) {
                    eventWithToken = iterator.next();
                    minTimestampInSegment = Math.min(minTimestampInSegment, eventWithToken.getEvent().getTimestamp());
                    if (eventWithToken.getToken() >= minToken
                            && eventWithToken.getEvent().getTimestamp() >= minTimestamp
                            && !consumer.test(eventWithToken.asEventWithToken())) {
                        return;
                    }
                }
                //TODO
                if (minToken > segment || minTimestampInSegment < minTimestamp) return;

            });
        }

        if( next != null) {
            next.query(minToken, minTimestamp, consumer);
        }
    }

    protected EventIterator createEventIterator(EventSource e, long segment, long startToken) {
        return e.createEventIterator(segment, startToken);
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier) {
        return getLastSequenceNumber(aggregateIdentifier, Integer.MAX_VALUE);
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
    public Optional<Event> getLastEvent(String aggregateId, long minSequenceNumber) {
        for (Long segment : getSegments()) {
            SortedSet<PositionInfo> positionInfos = getPositions(segment, aggregateId);
            if (positionInfos != null ) {
                PositionInfo last = positionInfos.last();
                if( last.getAggregateSequenceNumber() < minSequenceNumber) {
                    return Optional.empty();
                }
                Optional<EventSource> buffer = getEventSource(segment);
                if( buffer.isPresent()) {
                    Event event = buffer.get().readEvent(last.getPosition());
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
        if( validate) validate(storageProperties.getValidationSegments());
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
        Iterator<TransactionWithToken> iterator = getTransactions(segment, segment, true);
        try {
            TransactionWithToken last = null;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            return new ValidationResult(segment, last == null ? segment : last.getToken() + last.getEventsCount());
        } catch (Exception ex) {
            return new ValidationResult(segment, ex.getMessage());
        }
    }

    @Override
    public EventTypeContext getType() {
        return type;
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
    public void streamEvents(long token, StorageCallback storageCallback, Predicate<EventWithToken> onEvent)  {
        long lastSegment = -1;
        long segment = getSegmentFor(token);
        EventInformation eventWithToken = null;
        try {
            while (segment > lastSegment) {
                EventIterator eventIterator = getEvents(segment, token);
                while (eventIterator.hasNext()) {
                    eventWithToken = eventIterator.next();
                    if (!onEvent.test(eventWithToken.asEventWithToken())) {
                        eventIterator.close();
                        storageCallback.onCompleted(eventWithToken.getToken());
                        return;
                    }
                }
                lastSegment = segment;
                segment = getSegmentFor(eventWithToken == null ? token : eventWithToken.getToken() + 1);
                token = segment;
            }
            storageCallback.onCompleted(eventWithToken == null ? token: eventWithToken.getToken()+1);
        } catch (RuntimeException throwable) {
            storageCallback.onError(throwable);
        }

    }

    @Override
    public void streamTransactions(long token, StorageCallback storageCallback, Predicate<TransactionWithToken> onEvent)  {
        logger.debug("{}: Start streaming {} transactions at {}", context, type.getEventType(), token);

        long lastSegment = -1;
        long segment = getSegmentFor(token);
        TransactionWithToken eventWithToken = null;
        while (segment > lastSegment) {
            TransactionIterator transactionIterator = getTransactions(segment, token);
            while (transactionIterator.hasNext()) {
                eventWithToken = transactionIterator.next();
                token += eventWithToken.getEventsCount();
                if( ! onEvent.test(eventWithToken)) {
                    transactionIterator.close();
                    logger.debug("{}: Done streaming {} transactions due to false result", context, type.getEventType());
                    return;
                }
            }
            lastSegment = segment;
            segment = getSegmentFor(token);
        }
        logger.debug("{}: Done streaming event transactions", context);
        storageCallback.onCompleted(eventWithToken == null ? token : eventWithToken.getToken() + eventWithToken.getEventsCount());
    }

    @Override
    public boolean replicated() {
        return true;
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

    private void retrieveEventsForAnAggregate(long segment, SortedSet<PositionInfo> positions, Consumer<Event> onEvent) {
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

    protected boolean isDomainEvent(Event e) {
        return ! StringUtils.isEmpty(e.getAggregateIdentifier());
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
}
