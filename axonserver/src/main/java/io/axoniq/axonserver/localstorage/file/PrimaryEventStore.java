/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.FileSystemMonitor;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.StorageCallback;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import io.axoniq.axonserver.metric.MeterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages the writable segments of the event store. Once the segment is completed this class hands the segment over
 * to the next segment based event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class PrimaryEventStore extends SegmentBasedEventStore {

    protected static final Logger logger = LoggerFactory.getLogger(PrimaryEventStore.class);

    protected final EventTransformerFactory eventTransformerFactory;
    protected final Synchronizer synchronizer;
    protected final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    protected final AtomicLong lastToken = new AtomicLong(-1);
    //
    protected final ConcurrentNavigableMap<Long, ByteBufferEventSource> readBuffers = new ConcurrentSkipListMap<>();
    protected EventTransformer eventTransformer;
    protected final FileSystemMonitor fileSystemMonitor;

    /**
     * @param context                 the context and the content type (events or snapshots)
     * @param indexManager            the index manager to use
     * @param eventTransformerFactory the transformer factory
     * @param storageProperties       configuration of the storage engine
     * @param meterFactory            factory to create metrics meters
     * @param fileSystemMonitor
     */
    public PrimaryEventStore(EventTypeContext context, IndexManager indexManager,
                             EventTransformerFactory eventTransformerFactory, StorageProperties storageProperties,
                             SegmentBasedEventStore completedSegmentsHandler,
                             MeterFactory meterFactory, FileSystemMonitor fileSystemMonitor) {
        super(context, indexManager, storageProperties, completedSegmentsHandler, meterFactory);
        this.eventTransformerFactory = eventTransformerFactory;
        this.fileSystemMonitor = fileSystemMonitor;
        synchronizer = new Synchronizer(context, storageProperties, this::completeSegment);
    }

    @Override
    public void initSegments(long lastInitialized, long defaultFirstIndex) {
        File storageDir = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(storageDir);
        indexManager.init();
        eventTransformer = eventTransformerFactory.get(storageProperties.getFlags());
        initLatestSegment(lastInitialized, Long.MAX_VALUE, storageDir, defaultFirstIndex);

        fileSystemMonitor.registerPath(storageDir.toPath());
    }

    private void initLatestSegment(long lastInitialized, long nextToken, File storageDir, long defaultFirstIndex) {
        long first = getFirstFile(lastInitialized, storageDir, defaultFirstIndex);
        renameFileIfNecessary(first);
        first = firstSegmentIfLatestCompleted(first);
        if (next != null) {
            next.initSegments(first);
        }
        WritableEventSource buffer = getOrOpenDatafile(first);
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
        WritePosition writePosition = new WritePosition(sequence, buffer.position(), buffer, first);
        writePositionRef.set(writePosition);
        synchronizer.init(writePosition);
    }

    private long firstSegmentIfLatestCompleted(long latestSegment) {
        if (!indexManager.validIndex(latestSegment)) {
            return latestSegment;
        }
        WritableEventSource buffer = getOrOpenDatafile(latestSegment);
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

    private long getFirstFile(long lastInitialized, File events, long defaultFirstIndex) {
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
                private final AtomicBoolean execute = new AtomicBoolean(true);

                @Override
                public boolean onCompleted(long firstToken) {
                    if (execute.getAndSet(false)) {
                        indexManager.addToActiveSegment(writePosition.segment, indexEntries);
                        completableFuture.complete(firstToken);
                        lastToken.set(firstToken + preparedTransaction.getEventList().size() - 1);
                        return true;
                    }
                    return false;
                }

                @Override
                public void onError(Throwable cause) {
                    completableFuture.completeExceptionally(cause);
                }
            });
            write(writePosition, preparedTransaction.getEventSize(), preparedTransaction.getEventList(), indexEntries);
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
    public void close(boolean deleteData) {
        File storageDir = new File(storageProperties.getStorage(context));
        fileSystemMonitor.unregisterPath(storageDir.toPath());

        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> {
            source.clean(0);
            if( deleteData) removeSegment(s);
        });

        if( next != null) next.close(deleteData);

        indexManager.cleanup(deleteData);
        if (deleteData) {
            storageDir.delete();
        }
        closeListeners.forEach(Runnable::run);
    }

    @Override
    protected NavigableSet<Long> getSegments() {
        return readBuffers.descendingKeySet();
    }

    @Override
    public Optional<EventSource> getEventSource(long segment) {
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
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        return next != null ? next.getBackupFilenames(lastSegmentBackedUp) : Stream.empty();
    }

    @Override
    public void rollback(long token) {
        if (token >= getLastToken()) {
            return;
        }
        synchronizer.shutdown(false);
        NavigableSet<Long> segments = getSegments();

        if (segments.first() < token) {
            int currentPosition = writePositionRef.get().position;
            initLatestSegment(Long.MAX_VALUE, token + 1, new File(storageProperties.getStorage(context)), 0L);
            writePositionRef.get().buffer.clearTo(currentPosition);
        } else {

            for (long segment : getSegments()) {
                if (segment > token && segment > 0) {
                    removeSegment(segment);
                }
            }

            if (segments.isEmpty() && next != null) {
                next.rollback(token);
            }

            initLatestSegment(Long.MAX_VALUE, token + 1, new File(storageProperties.getStorage(context)), 0L);
            writePositionRef.get().buffer.clearTo(writePositionRef.get().buffer.capacity());
        }
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {

        return new CloseableIterator<SerializedEventWithToken>() {

            long nextToken = start;
            EventIterator eventIterator;

            @Override
            public void close() {
                if (eventIterator != null) {
                    eventIterator.close();
                }
            }

            @Override
            public boolean hasNext() {
                return nextToken <= getLastToken();
            }

            @Override
            public SerializedEventWithToken next() {
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

    @Override
    protected void recreateIndex(long segment) {
        // No implementation as for primary segment store there are no index files, index is kept in memory
    }

    private void removeSegment(long segment) {
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
            next.handover(writePosition.segment, () -> {
                ByteBufferEventSource source = readBuffers.remove(writePosition.segment);
                logger.debug("Handed over {}, remaining segments: {}",
                             writePosition.segment,
                             getSegments());
                if (source != null) {
                    source.clean(storageProperties.getPrimaryCleanupDelay());
                }
            });
        }
    }

    private void write(WritePosition writePosition, int eventSize, List<ProcessedEvent> eventList,
                       Map<String, List<IndexEntry>> indexEntries) {
        ByteBufferEventSource source = writePosition.buffer.duplicate();
        ByteBuffer writeBuffer = source.getBuffer();
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(0);
        writeBuffer.put(TRANSACTION_VERSION);
        writeBuffer.putShort((short) eventList.size());
        long token = writePosition.sequence;
        Checksum checksum = new Checksum();
        int eventsPosition = writeBuffer.position();
        for (ProcessedEvent event : eventList) {
            int position = writeBuffer.position();
            writeBuffer.putInt(event.getSerializedSize());
            writeBuffer.put(event.toByteArray());
            if (event.isDomainEvent()) {
                indexEntries.computeIfAbsent(event.getAggregateIdentifier(),
                                             k -> new ArrayList<>())
                            .add(new IndexEntry(event.getAggregateSequenceNumber(), position, token));
            }
            token++;
        }

        writeBuffer.putInt(checksum.update(writeBuffer, eventsPosition, writeBuffer.position() - eventsPosition).get());
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(eventSize);
        source.close();
    }

    private WritePosition claim(int eventBlockSize, int nrOfEvents) {
        int totalSize = HEADER_BYTES + eventBlockSize + TX_CHECKSUM_BYTES;
        if (totalSize > storageProperties.getSegmentSize() - 9) {
            throw new MessagingPlatformException(ErrorCode.PAYLOAD_TOO_LARGE,
                                                 "Size of transaction too large, max size = " + (
                                                         storageProperties.getSegmentSize() - 9));
        }
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

    protected WritableEventSource getOrOpenDatafile(long segment) {
        File file = storageProperties.dataFile(context, segment);
        long size = storageProperties.getSegmentSize();
        if (file.exists()) {
            size = file.length();
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
        int size = 0;
        for (ProcessedEvent event : eventList) {
            size += 4 + event.getSerializedSize();
        }
        return size;
    }
}
