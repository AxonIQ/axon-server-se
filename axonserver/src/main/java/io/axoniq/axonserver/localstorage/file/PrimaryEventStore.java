/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.StorageCallback;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import io.axoniq.axonserver.metric.MeterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.localstorage.file.FileUtils.name;

/**
 * Manages the writable segments of the event store. Once the segment is completed this class hands the segment over
 * to the next segment based event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class PrimaryEventStore extends SegmentBasedEventStore {

    protected static final Logger logger = LoggerFactory.getLogger(PrimaryEventStore.class);
    public static final int MAX_EVENTS_PER_BLOCK = Short.MAX_VALUE;

    protected final EventTransformerFactory eventTransformerFactory;
    protected final Synchronizer synchronizer;
    protected final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    protected final AtomicLong lastToken = new AtomicLong(-1);
    protected final ConcurrentNavigableMap<Long, ByteBufferEventSource> readBuffers = new ConcurrentSkipListMap<>();
    protected EventTransformer eventTransformer;
    protected final FileSystemMonitor fileSystemMonitor;

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
        super(context, indexManager, storagePropertiesSupplier, completedSegmentsHandler, meterFactory);
        this.eventTransformerFactory = eventTransformerFactory;
        this.fileSystemMonitor = fileSystemMonitor;
        synchronizer = new Synchronizer(context, storagePropertiesSupplier.get(), this::completeSegment);
    }

    @Override
    public void initSegments(long lastInitialized, long defaultFirstIndex) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File storageDir = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(storageDir);
        indexManager.init();
        eventTransformer = eventTransformerFactory.get(storageProperties.getFlags());
        initLatestSegment(lastInitialized, Long.MAX_VALUE, storageDir, defaultFirstIndex, storageProperties);

        fileSystemMonitor.registerPath(storeName(), storageDir.toPath());
    }

    private void initLatestSegment(long lastInitialized, long nextToken, File storageDir, long defaultFirstIndex,
                                   StorageProperties storageProperties) {
        FileVersion first = getFirstFile(lastInitialized, storageDir, new FileVersion(defaultFirstIndex, 0), storageProperties);
        renameFileIfNecessary(first.segment());
        first = firstSegmentIfLatestCompleted(first, storageProperties);
        if (next != null) {
            next.initSegments(first.segment());
        }
        WritableEventSource buffer = getOrOpenDatafile(first, storageProperties.getSegmentSize(), false);
        indexManager.remove(first);
        long sequence = first.segment();
        Map<String, List<IndexEntry>> loadedEntries = new HashMap<>();
        try (EventByteBufferIterator iterator = new EventByteBufferIterator(buffer, first.segment())) {
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

        indexManager.addToActiveSegment(first.segment(), loadedEntries);

        buffer.putInt(buffer.position(), 0);
        WritePosition writePosition = new WritePosition(sequence,
                                                        buffer.position(),
                                                        first.version(),
                                                        buffer,
                                                        first.segment(),
                                                        0);
        writePositionRef.set(writePosition);
        synchronizer.init(writePosition);
    }

    public int activeSegmentCount() {
        return readBuffers.size();
    }

    private FileVersion firstSegmentIfLatestCompleted(FileVersion latestSegment, StorageProperties storageProperties) {
        if (!indexManager.validIndex(latestSegment)) {
            return latestSegment;
        }
        WritableEventSource buffer = getOrOpenDatafile(latestSegment, storageProperties.getSegmentSize(), false);
        long token = latestSegment.segment();
        try (EventIterator iterator = buffer.createEventIterator(latestSegment.segment())) {
            while (iterator.hasNext()) {
                iterator.next();
                token++;
            }
        } finally {
            readBuffers.remove(latestSegment.segment());
            buffer.close();
        }
        return new FileVersion(token, 0);
    }

    private FileVersion getFirstFile(long lastInitialized, File events, FileVersion defaultFirstIndex,
                              StorageProperties storageProperties) {
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getEventsSuffix());

        return Arrays.stream(eventFiles)
                     .map(name -> FileUtils.process(name))
                     .filter(segment -> segment.segment() < lastInitialized)
                     .max(FileVersion::compareTo)
                     .orElse(defaultFirstIndex);
    }

    private FilePreparedTransaction prepareTransaction(List<Event> origEventList, int version) {
        List<ProcessedEvent> eventList = origEventList.stream().map(s -> new WrappedEvent(s, eventTransformer)).collect(
                Collectors.toList());
        int eventSize = eventBlockSize(eventList);
        WritePosition writePosition = claim(eventSize, eventList.size(), version);
        return new FilePreparedTransaction(writePosition, eventSize, eventList);
    }

    /**
     * Stores a list of events. Completable future completes when these events and all previous events are written.
     *
     * @param events the events to store
     * @return completable future with the token of the first event
     */
    @Override
    public CompletableFuture<Long> store(List<Event> events, int version) {

        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            Map<String, List<IndexEntry>> indexEntries = new HashMap<>();
            FilePreparedTransaction preparedTransaction = prepareTransaction(events, version);
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
    public void handover(FileVersion segment, Runnable callback) {
        callback.run();
    }

    @Override
    protected boolean containsSegment(long segment) {
        return readBuffers.containsKey(segment);
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

        if( next != null) next.close(deleteData);

        indexManager.cleanup(deleteData);
        if (deleteData) {
            FileUtils.delete(storageDir);
        }
        closeListeners.forEach(Runnable::run);
    }

    @Override
    protected NavigableSet<Long> getSegments() {
        return readBuffers.descendingKeySet();
    }

    @Override
    public Optional<EventSource> getEventSource(FileVersion segment) {
        return getEventSource(segment.segment());
    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        if (readBuffers.containsKey(segment)) {
            return Optional.of(readBuffers.get(segment).duplicate());
        }
        return Optional.empty();
    }

    @Override
    protected boolean removeSegment(long segment, int version) {
        return next.removeSegment(segment, version);
    }

    @Override
    protected Integer currentSegmentVersion(Long segment) {
        Integer version = next.currentSegmentVersion(segment);
        return version == null ? 0 : version;
    }

    @Override
    protected void activateSegmentVersion(long segment, int version) {
        // no-op, no versioning for primary segments
        if (next != null) {
            next.activateSegmentVersion(segment, version);
        }
    }

    public Flux<TransformationProgress> transformContents(int newVersion, Flux<EventWithToken> transformedEvents) {
        // TODO: 7/26/22 revisit this approach!!!
        return Mono.fromRunnable(this::forceNextSegment)
                   .thenMany(super.transformContents(newVersion, transformedEvents));
    }

    private void forceNextSegment() {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        WritePosition writePosition = writePositionRef //replace the reference with a fake write position
                                                       .getAndUpdate(prev -> prev.incrementedWith(0,
                                                                                                  storageProperties.getSegmentSize(),
                                                                                                  0));
        //this if is needed to allow only 1 thread to enter (the others will have the fake write position)
        if (writePosition.isOverflow(storageProperties.getSegmentSize())) {
            // only one thread can be here
            logger.debug("{}: Creating new segment {}", context, writePosition.sequence);

            writePosition.buffer.putInt(writePosition.position, -1); //writePosition.writeEndOfFile();

            //create new file
            WritableEventSource buffer = getOrOpenDatafile(new FileVersion(writePosition.sequence, 0),
                                                           storageProperties.getSegmentSize(),
                                                           false);
            //replace the reference with a valid write position
            writePositionRef.set(writePosition.reset(buffer, 0));

            //the synchronizer close the previous one
            synchronizer.register(new WritePosition(writePosition.sequence,
                                                    0,
                                                    0,
                                                    buffer,
                                                    writePosition.sequence,
                                                    writePosition.prevEntries),
                                  new StorageCallback() {
                                      @Override
                                      public boolean complete(long firstToken) {
                                          logger.warn("Ready for transformation");
                                          return true;
                                      }

                                      @Override
                                      public void error(Throwable cause) {

                                      }
                                  });
            synchronizer.notifyWritePositions();
            waitForPendingFileCompletions();
        }
    }

    private void waitForPendingFileCompletions() {
        while (readBuffers.size() != 1) {
            try {
                //noinspection BusyWait
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public long getLastToken() {
        return lastToken.get();
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp, boolean includeActive) {
        if (includeActive) {
            StorageProperties storageProperties = storagePropertiesSupplier.get();
            Stream<String> filenames = getSegments().stream()
                                                    .map(s -> name(storageProperties.dataFile(context, s)));
            return next != null ?
                    Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp, includeActive)) :
                    filenames;

        }
        return next != null ? next.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp, includeActive) : Stream.empty();
    }

    @Override
    public long getFirstCompletedSegment() {
         return next == null ? -1 : next.getFirstCompletedSegment();
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
                if (closed.get()) throw new IllegalStateException("Iterator is closed");
                return nextToken <= getLastToken();
            }

            @Override
            public SerializedEventWithToken next() {
                if (closed.get()) throw new IllegalStateException("Iterator is closed");
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
    protected void recreateIndex(FileVersion segment) {
        // No implementation as for primary segment store there are no index files, index is kept in memory
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
        indexManager.complete(new FileVersion(writePosition.segment, 0));
        if (next != null) {
            next.handover(new FileVersion(writePosition.segment, 0), () -> {
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

    private WritePosition claim(int eventBlockSize, int nrOfEvents, int version) {
        int blocks = (int) Math.ceil(nrOfEvents / (double) MAX_EVENTS_PER_BLOCK);
        int totalSize = eventBlockSize + blocks * (HEADER_BYTES + TX_CHECKSUM_BYTES);
        if (totalSize > MAX_TRANSACTION_SIZE || eventBlockSize <= 0) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 String.format("Illegal transaction size: %d", eventBlockSize));
        }
        WritePosition writePosition;
        do {
            writePosition = writePositionRef.getAndUpdate(prev -> prev.incrementedWith(nrOfEvents, totalSize, version));

            if (writePosition.isOverflow(totalSize) || writePosition.isVersionUpdate(version)) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", context, writePosition.sequence);

                writePosition.buffer.putInt(writePosition.position, -1);

                WritableEventSource buffer = getOrOpenDatafile(new FileVersion(writePosition.sequence, 0),
                                                               totalSize + FILE_HEADER_SIZE + FILE_FOOTER_SIZE,
                                                               true);
                writePositionRef.set(writePosition.reset(buffer, version));
            }
        } while (!writePosition.isWritable(totalSize) || writePosition.isVersionUpdate(version));

        return writePosition;
    }

    @Override
    public long nextToken() {
        return writePositionRef.get().sequence;
    }

    protected WritableEventSource getOrOpenDatafile(FileVersion segment, int minSize, boolean canReplaceFile) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File file = storageProperties.dataFile(context, segment);
        int size = Math.max(storageProperties.getSegmentSize(), minSize);
        if (file.exists()) {
            if (canReplaceFile && file.length() < minSize) {
                ByteBufferEventSource s = readBuffers.remove(segment.segment());
                if (s != null) {
                    s.clean(0);
                }
                FileUtils.delete(file);
            } else {
                size = (int) file.length();
            }
        } else if (segment.version() > 0) {
            File defaultFile = storageProperties.dataFile(context, new FileVersion(segment.segment(), 0));
            if (defaultFile.exists()) {
                ByteBufferEventSource s = readBuffers.remove(segment.segment());
                if (s != null) {
                    s.clean(0);
                }
                FileUtils.delete(defaultFile);
            }
        }

        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            logger.info("Opening file {}", file);
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buffer.put(VERSION);
            buffer.putInt(storageProperties.getFlags());
            WritableEventSource writableEventSource = new WritableEventSource(file.getAbsolutePath(),
                                                                              buffer,
                                                                              segment.segment(),
                                                                              segment.version(),
                                                                              eventTransformer,
                                                                              storageProperties.isCleanRequired());
            readBuffers.put(segment.segment(), writableEventSource);
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
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, "Transaction size exceeds maximum size");
        }
        return (int) size;
    }

    private String storeName() {
        return context + "-" + type.getEventType().name().toLowerCase();
    }

    public Set<Long> activeSegments() {
        return readBuffers.keySet();
    }
}
