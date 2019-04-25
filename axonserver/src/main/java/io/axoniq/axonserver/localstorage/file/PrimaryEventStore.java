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
import io.axoniq.axonserver.localstorage.EventInformation;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.StorageCallback;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 */
public class PrimaryEventStore extends SegmentBasedEventStore {
    private static final Logger logger = LoggerFactory.getLogger(PrimaryEventStore.class);

    private final EventTransformerFactory eventTransformerFactory;
    private final Synchronizer synchronizer;
    private final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    private final AtomicLong lastToken = new AtomicLong(-1);
    private final ConcurrentNavigableMap<Long, Map<String, SortedSet<PositionInfo>>> positionsPerSegmentMap = new ConcurrentSkipListMap<>();
    private final Map<Long, ByteBufferEventSource> readBuffers = new ConcurrentHashMap<>();
    private EventTransformer eventTransformer;

    public PrimaryEventStore(EventTypeContext context, IndexManager indexCreator, EventTransformerFactory eventTransformerFactory, StorageProperties storageProperties) {
        super(context, indexCreator, storageProperties);
        this.eventTransformerFactory = eventTransformerFactory;
        synchronizer = new Synchronizer(context, storageProperties, this::completeSegment);
    }

    @Override
    public void initSegments(long lastInitialized) {
        File storageDir  = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(storageDir);
        eventTransformer = eventTransformerFactory.get(VERSION, storageProperties.getFlags(), storageProperties);
        initLatestSegment(lastInitialized, Long.MAX_VALUE, storageDir);
    }

    private void initLatestSegment(long lastInitialized, long nextToken, File storageDir) {
        long first = getFirstFile(lastInitialized, storageDir);
        renameFileIfNecessary(first);
        WritableEventSource buffer = getOrOpenDatafile(first);
        FileUtils.delete(storageProperties.index(context, first));
        FileUtils.delete(storageProperties.bloomFilter(context, first));
        long sequence = first;
        try (EventByteBufferIterator iterator = new EventByteBufferIterator(buffer, first, first) ) {
            Map<String, SortedSet<PositionInfo>> aggregatePositions = new ConcurrentHashMap<>();
            positionsPerSegmentMap.put(first, aggregatePositions);
            while (sequence < nextToken && iterator.hasNext()) {
                EventInformation event = iterator.next();
                if (isDomainEvent(event.getEvent())) {
                    aggregatePositions.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
                                                       k -> new ConcurrentSkipListSet<>())
                                      .add(new PositionInfo(event.getPosition(),
                                                            event.getEvent().getAggregateSequenceNumber()));

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
                    if (isDomainEvent(event.getEvent())) {
                        aggregatePositions.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
                                                           k -> new ConcurrentSkipListSet<>())
                                          .add(new PositionInfo(event.getPosition(),
                                                                event.getEvent().getAggregateSequenceNumber()));

                    }
                    sequence++;
                }
            }
            lastToken.set(sequence - 1);
        }

        buffer.putInt(buffer.position(), 0);
        WritePosition writePosition = new WritePosition(sequence, buffer.position(), buffer, first);
        writePositionRef.set(writePosition);
        synchronizer.init(writePosition);

        if( next != null) {
            next.initSegments(first);
        }
    }

    private long getFirstFile(long lastInitialized, File events) {
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());

        return Arrays.stream(eventFiles)
                     .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
                     .filter(segment -> segment < lastInitialized)
                     .max(Long::compareTo)
                     .orElse(0L);
    }

    @Override
    public FilePreparedTransaction prepareTransaction( List<SerializedEvent> origEventList) {
        List<ProcessedEvent>eventList = origEventList.stream().map(s -> new WrappedEvent(s, eventTransformer)).collect(
                Collectors.toList());
        int eventSize = eventBlockSize(eventList);
        WritePosition writePosition = claim(eventSize, eventList.size());
        return new FilePreparedTransaction(writePosition, eventSize, eventList);
    }

    @Override
    public CompletableFuture<Long> store(PreparedTransaction basePreparedTransaction) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            FilePreparedTransaction preparedTransaction = (FilePreparedTransaction)basePreparedTransaction;
            List<ProcessedEvent> eventList = preparedTransaction.getEventList();
            int eventSize = preparedTransaction.getEventSize();
            WritePosition writePosition = preparedTransaction.getWritePosition();
            synchronizer.register(writePosition, new StorageCallback() {
                private final AtomicBoolean execute = new AtomicBoolean(true);

                @Override
                public boolean onCompleted(long firstToken) {
                    if( execute.getAndSet(false)) {
                        completableFuture.complete(firstToken);
                        lastToken.set(firstToken + eventList.size() -1);
                        return true;
                    }
                    return false;
                }

                @Override
                public void onError(Throwable cause) {
                    completableFuture.completeExceptionally(cause);
                }
            });
            write(writePosition, eventSize, eventList);
            synchronizer.notifyWritePositions();
        } catch (RuntimeException cause) {
            completableFuture.completeExceptionally(cause);
        }

        return completableFuture;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        callback.run();
    }

    @Override
    public void close() {
        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> source.clean(5));
        if( next != null) next.close();
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return positionsPerSegmentMap.descendingKeySet();
    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        if( readBuffers.containsKey(segment) ) {
            return Optional.of(readBuffers.get(segment).duplicate());
        }
        return Optional.empty();
    }

    @Override
    protected SortedSet<PositionInfo> getPositions(long segment, String aggregateId) {
        return positionsPerSegmentMap.get(segment).get(aggregateId);
    }

    @Override
    public long getLastToken() {
        return lastToken.get();
    }

    private AtomicLong lastSequenceForAggregate(String aggregateId, boolean checkAll) {
        return new AtomicLong( getLastSequenceNumber(aggregateId,
                                                     checkAll ? Integer.MAX_VALUE : MAX_SEGMENTS_FOR_SEQUENCE_NUMBER_CHECK).orElse(-1L));
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        return next!= null ? next.getBackupFilenames(lastSegmentBackedUp): Stream.empty();
    }

    @Override
    public void rollback( long token) {
        if( token >= getLastToken() ) {
            return;
        }
        synchronizer.shutdown(false);

        if( positionsPerSegmentMap.descendingKeySet().first() < token) {
            int currentPosition = writePositionRef.get().position;
            initLatestSegment(Long.MAX_VALUE, token+1, new File(storageProperties.getStorage(context)));
            writePositionRef.get().buffer.clearTo(currentPosition);
        } else {

            for (long segment : getSegments()) {
                if (segment > token && segment > 0) {
                    removeSegment(segment);
                }
            }

            if (positionsPerSegmentMap.isEmpty() && next != null) {
                next.rollback(token);
            }

            initLatestSegment(Long.MAX_VALUE, token + 1, new File(storageProperties.getStorage(context)));
            writePositionRef.get().buffer.clearTo(writePositionRef.get().buffer.capacity());
        }
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {

        return new CloseableIterator<SerializedEventWithToken>() {

            @Override
            public void close() {
                if( eventIterator != null) eventIterator.close();
            }

            long nextToken = start;
            EventIterator eventIterator;

            @Override
            public boolean hasNext() {
                return nextToken <= getLastToken();
            }

            @Override
            public SerializedEventWithToken next() {
                if( eventIterator == null) {
                    eventIterator = getEvents(getSegmentFor(nextToken), nextToken);
                }
                SerializedEventWithToken event = null;
                if( eventIterator.hasNext() ) {
                    event = eventIterator.next().getSerializedEventWithToken();
                } else {
                    eventIterator.close();
                    eventIterator = getEvents(getSegmentFor(nextToken), nextToken);
                    if( eventIterator.hasNext()) {
                        event = eventIterator.next().getSerializedEventWithToken();
                    }
                }
                if( event != null) {
                    nextToken = event.getToken() + 1;
                    return event;
                }
                throw new NoSuchElementException("No event for token " + nextToken);
            }
        };
    }

    @Override
    protected void recreateIndex(long segment) {
        // No implementation as for primary segment store there are no index files, index is kept in memory
    }

    private void removeSegment(long segment) {
        positionsPerSegmentMap.remove(segment);
        ByteBufferEventSource eventSource = readBuffers.remove(segment);
        if( eventSource != null) eventSource.clean(0);
        FileUtils.delete(storageProperties.dataFile(context, segment));
    }

    private void completeSegment(WritePosition writePosition) {
        try {
            indexManager.createIndex(writePosition.segment, positionsPerSegmentMap.get(writePosition.segment), false);
        } catch( RuntimeException re) {
            logger.warn("Failed to create index", re);
        }
        if( next != null) {
            next.handover(writePosition.segment, () -> {
                positionsPerSegmentMap.remove(writePosition.segment);
                ByteBufferEventSource source = readBuffers.remove(writePosition.segment);
                logger.debug("Handed over {}, remaining segments: {}", writePosition.segment, positionsPerSegmentMap.keySet());
                source.clean(storageProperties.getPrimaryCleanupDelay());
            });
        }
    }

    private void write(WritePosition writePosition, int eventSize, List<ProcessedEvent> eventList) {
        ByteBuffer writeBuffer = writePosition.buffer.duplicate().getBuffer();
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(0);
        writeBuffer.put(TRANSACTION_VERSION);
        writeBuffer.putShort((short) eventList.size());
        Checksum checksum = new Checksum();
        int eventsPosition = writeBuffer.position();
        for( ProcessedEvent event : eventList) {
            int position = writeBuffer.position();
            writeBuffer.putInt(event.getSerializedSize());
            writeBuffer.put(event.toByteArray());
            if( event.isDomainEvent()) {
                positionsPerSegmentMap.get(writePosition.segment).computeIfAbsent(event.getAggregateIdentifier(),
                                                                                  k -> new ConcurrentSkipListSet<>())
                                      .add(new PositionInfo(position, event.getAggregateSequenceNumber()));
            }
        }

        writeBuffer.putInt(checksum.update(writeBuffer, eventsPosition, writeBuffer.position() - eventsPosition).get());
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(eventSize);
    }

    private WritePosition claim(int eventBlockSize, int nrOfEvents)  {
        int totalSize = HEADER_BYTES + eventBlockSize + TX_CHECKSUM_BYTES;
        if( totalSize > storageProperties.getSegmentSize()-9)
            throw new MessagingPlatformException(ErrorCode.PAYLOAD_TOO_LARGE, "Size of transaction too large, max size = " + (storageProperties.getSegmentSize() - 9));
        WritePosition writePosition;
        do {
            writePosition = writePositionRef.getAndAccumulate(
                    new WritePosition(nrOfEvents, totalSize),
                    (prev, x) -> prev.incrementedWith(x.sequence, x.position));

            if (writePosition.isOverflow(totalSize)) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", context, writePosition.sequence);

                writePosition.buffer.putInt(writePosition.position, -1);

                WritableEventSource buffer = getOrOpenDatafile(writePosition.sequence);
                writePositionRef.set(writePosition.reset(buffer));
            }
        } while (!writePosition.isWritable(totalSize));

        return writePosition;
    }

    @Override
    public long nextToken() {
        return writePositionRef.get().sequence;
    }

    @Override
    public void deleteAllEventData() {
        rollback(-1);
    }

    private WritableEventSource getOrOpenDatafile(long segment)  {
        File file= storageProperties.dataFile(context, segment);
        long size = storageProperties.getSegmentSize();
        if( file.exists()) {
            size = file.length();
        }
        try(FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            positionsPerSegmentMap.computeIfAbsent(segment, k -> new ConcurrentHashMap<>());
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buffer.put(VERSION);
            buffer.putInt(storageProperties.getFlags());
            WritableEventSource writableEventSource = new WritableEventSource(file.getAbsolutePath(), buffer, eventTransformer);
            readBuffers.put(segment, writableEventSource);
            return writableEventSource;
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Failed to open segment: " + segment, ioException);
        }
    }

    private int eventBlockSize(List<ProcessedEvent> eventList) {
        int size = 0;
        for( ProcessedEvent event : eventList) {
            size += 4 + event.getSerializedSize();
        }
        return size;
    }

    private class MinMaxPair {

        private final String key;
        private final long min;
        private volatile long max;

        MinMaxPair(String key, long min) {
            this.key = key;
            this.min = min;
            this.max = min-1;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public void setMax(long max) {
            if( max != this.max + 1) {
                throw new MessagingPlatformException(ErrorCode.INVALID_SEQUENCE, String.format("Invalid sequence number %d for aggregate %s, expected %d",
                                                                                               max, key, this.max+1));

            }
            this.max = max;
        }
    }
}
