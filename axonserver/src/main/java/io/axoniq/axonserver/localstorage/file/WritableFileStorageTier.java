/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.StorageCallback;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import io.axoniq.axonserver.metric.MeterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.localstorage.file.FileEventStorageEngine.MAX_EVENTS_PER_BLOCK;
import static io.axoniq.axonserver.localstorage.file.FileUtils.name;

public class WritableFileStorageTier extends AbstractFileStorageTier {

    private static final Logger logger = LoggerFactory.getLogger(WritableFileStorageTier.class);

    private final Synchronizer synchronizer;
    private final FileSystemMonitor fileSystemMonitor;
    private final EventTransformer eventTransformer;

    protected final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    protected final AtomicLong lastToken = new AtomicLong(-1);
    protected final ConcurrentNavigableMap<Long, ByteBufferEventSource> readBuffers = new ConcurrentSkipListMap<>(
            Comparator.reverseOrder());


    public WritableFileStorageTier(EventTypeContext eventTypeContext, IndexManager indexManager,
                                   Supplier<StorageProperties> storagePropertiesSupplier,
                                   Supplier<StorageTier> completedSegmentsHandler, MeterFactory meterFactory,
                                   String storagePath, EventTransformerFactory eventTransformerFactory,
                                   FileSystemMonitor fileSystemMonitor) {
        super(eventTypeContext,
              indexManager,
              storagePropertiesSupplier,
              completedSegmentsHandler,
              meterFactory,
              storagePath);
        this.fileSystemMonitor = fileSystemMonitor;
        eventTransformer = eventTransformerFactory.get(storagePropertiesSupplier.get().getFlags());
        synchronizer = new Synchronizer(eventTypeContext, storagePropertiesSupplier.get(), this::completeSegment);
    }

    public void initSegments(long lastInitialized, long defaultFirstIndex) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File storageDir = new File(storagePath);
        FileUtils.checkCreateDirectory(storageDir);
        indexManager.init();
        initLatestSegment(lastInitialized, storageDir, defaultFirstIndex, storageProperties);

        fileSystemMonitor.registerPath(storeName(), storageDir.toPath());
    }

    private void initLatestSegment(long lastInitialized, File storageDir, long defaultFirstIndex,
                                   StorageProperties storageProperties) {
        FileVersion first = getFirstFile(lastInitialized,
                                         storageDir,
                                         new FileVersion(defaultFirstIndex, 0),
                                         storageProperties);
        renameFileIfNecessary(first.segment());
        FileVersion realFirst = firstSegmentIfLatestCompleted(first, storageProperties);
        applyOnNext(n -> n.initSegments(realFirst.segment()));
        createMissingIndexes();
        WritableEventSource buffer = getOrOpenDatafile(realFirst, storageProperties.getSegmentSize(), false);
        indexManager.remove(realFirst);
        long sequence = realFirst.segment();
        Map<String, List<IndexEntry>> loadedEntries = new HashMap<>();
        try (EventByteBufferIterator iterator = new EventByteBufferIterator(buffer, realFirst.segment())) {
            while (iterator.hasNext()) {
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
            lastToken.set(sequence - 1);
        }

        indexManager.addToActiveSegment(realFirst.segment(), loadedEntries);

        buffer.putInt(buffer.position(), 0);
        WritePosition writePosition = new WritePosition(sequence,
                                                        buffer.position(),
                                                        realFirst.segmentVersion(),
                                                        buffer,
                                                        realFirst.segment(),
                                                        0);
        writePositionRef.set(writePosition);
        synchronizer.init(writePosition);
    }

    private void createMissingIndexes() {
        SortedSet<FileVersion> segmentsWithoutIndex = segmentsWithoutIndex();
        segmentsWithoutIndex.forEach(this::createIndex);
    }

    @Override
    public SortedSet<FileVersion> segmentsWithoutIndex() {
        return invokeOnNext(StorageTier::segmentsWithoutIndex,
                            Collections.emptySortedSet());
    }

    private void createIndex(FileVersion segment) {
        Optional<EventSource> optionalEventSource = invokeOnNext(n -> n.eventSource(segment), Optional.empty());
        if (optionalEventSource.isPresent()) {
            try (EventIterator iterator = optionalEventSource.get().createEventIterator(segment.segment())) {
                Map<String, List<IndexEntry>> entries = new HashMap<>();
                while (iterator.hasNext()) {
                    EventInformation event = iterator.next();
                    if (event.isDomainEvent()) {
                        IndexEntry indexEntry = new IndexEntry(
                                event.getEvent().getAggregateSequenceNumber(),
                                event.getPosition(),
                                event.getToken());
                        entries.computeIfAbsent(event.getEvent().getAggregateIdentifier(), id -> new LinkedList<>())
                               .add(indexEntry);
                    }
                }

                indexManager.createIndex(segment, entries);
            }
        }
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
                     .map(FileUtils::process)
                     .filter(segment -> segment.segment() < lastInitialized)
                     .max(FileVersion::compareTo)
                     .orElse(defaultFirstIndex);
    }

    private FilePreparedTransaction prepareTransaction(List<Event> origEventList, int segmentVersion) {
        List<ProcessedEvent> eventList = origEventList.stream().map(s -> new WrappedEvent(s, eventTransformer)).collect(
                Collectors.toList());
        int eventSize = eventBlockSize(eventList);
        WritePosition writePosition = claim(eventSize, eventList.size(), segmentVersion);
        return new FilePreparedTransaction(writePosition, eventSize, eventList);
    }


    /**
     * Stores a list of events. Completable future completes when these events and all previous events are written.
     *
     * @param events the events to store
     * @return completable future with the token of the first event
     */
    public CompletableFuture<Long> store(List<Event> events, int segmentVersion) {

        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            Map<String, List<IndexEntry>> indexEntries = new HashMap<>();
            FilePreparedTransaction preparedTransaction = prepareTransaction(events, segmentVersion);
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

    protected WritableEventSource getOrOpenDatafile(FileVersion segment, int minSize, boolean canReplaceFile) {
        StorageProperties storageProperties = storagePropertiesSupplier.get();
        File file = storageProperties.dataFile(storagePath, segment);
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
        } else if (segment.segmentVersion() > 0) {
            File defaultFile = storageProperties.dataFile(storagePath, new FileVersion(segment.segment(), 0));
            if (defaultFile.exists()) {
                ByteBufferEventSource s = readBuffers.remove(segment.segment());
                if (s != null) {
                    s.clean(0);
                }
                FileUtils.delete(defaultFile);
            }
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            logger.info("Opening file {}", file);
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buffer.put(EVENT_FORMAT_VERSION);
            buffer.putInt(storageProperties.getFlags());
            WritableEventSource writableEventSource = new WritableEventSource(file.getAbsolutePath(),
                                                                              buffer,
                                                                              segment.segment(),
                                                                              segment.segmentVersion(),
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
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Transaction size exceeds maximum size");
        }
        return (int) size;
    }

    public void forceNextSegmentIfNeeded(long segment) {
        if (!readBuffers.containsKey(segment)) {
            return;
        }
        if (segment == readBuffers.firstKey()) {
            logger.info("Forcing next segment to be created");
            StorageProperties storageProperties = storagePropertiesSupplier.get();
            WritePosition writePosition = writePositionRef //replace the reference with a fake write position
                                                           .getAndUpdate(prev -> prev.incrementedWith(0,
                                                                                                      storageProperties.getSegmentSize(),
                                                                                                      0));
            //this if is needed to allow only 1 thread to enter (the others will have the fake write position)
            if (writePosition.isOverflow(storageProperties.getSegmentSize())) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", eventTypeContext, writePosition.sequence);

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
                                              logger.info("Opening new segment completed.");
                                              return true;
                                          }

                                          @Override
                                          public void error(Throwable cause) {

                                          }
                                      });
                synchronizer.notifyWritePositions();
            }
        }
        waitForPendingFileCompletions();
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

    private void completeSegment(WritePosition writePosition) {
        indexManager.complete(new FileVersion(writePosition.segment, 0));
        applyOnNext(n ->
                            n.handover(new Segment() {
                                @Override
                                public Supplier<InputStream> contentProvider() {
                                    return () -> null;
                                }

                                @Override
                                public FileVersion id() {
                                    return new FileVersion(writePosition.segment, writePosition.version);
                                }

                                @Override
                                public Supplier<List<File>> indexProvider() {
                                    return Collections::emptyList;
                                }

                                @Override
                                public Supplier<List<File>> previousVersions() {
                                    return Collections::emptyList;
                                }

                                @Override
                                public Stream<AggregateSequence> latestSequenceNumbers() {
                                    return Stream.empty();
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
                            }));
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

    private WritePosition claim(int eventBlockSize, int nrOfEvents, int segmentVersion) {
        int blocks = (int) Math.ceil(nrOfEvents / (double) MAX_EVENTS_PER_BLOCK);
        int totalSize = eventBlockSize + blocks * (HEADER_BYTES + TX_CHECKSUM_BYTES);
        if (totalSize > MAX_TRANSACTION_SIZE || eventBlockSize <= 0) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 String.format("Illegal transaction size: %d", eventBlockSize));
        }
        WritePosition writePosition;
        do {
            writePosition = writePositionRef.getAndUpdate(prev -> prev.incrementedWith(nrOfEvents,
                                                                                       totalSize,
                                                                                       segmentVersion));

            if (writePosition.isOverflow(totalSize) || writePosition.isVersionUpdate(segmentVersion)) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", eventTypeContext, writePosition.sequence);

                writePosition.buffer.putInt(writePosition.position, -1);

                WritableEventSource buffer = getOrOpenDatafile(new FileVersion(writePosition.sequence, segmentVersion),
                                                               totalSize + FILE_HEADER_SIZE + FILE_FOOTER_SIZE,
                                                               true);
                writePositionRef.set(writePosition.reset(buffer, segmentVersion));
            }
        } while (!writePosition.isWritable(totalSize) || writePosition.isVersionUpdate(segmentVersion));

        return writePosition;
    }

    public long nextToken() {
        return writePositionRef.get().sequence;
    }


    private String storeName() {
        return eventTypeContext + "-" + eventTypeContext.getEventType().name().toLowerCase();
    }


    public long getLastToken() {
        return lastToken.get();
    }

    public Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp, boolean includeActive) {
        if (includeActive) {
            Stream<String> filenames = readBuffers
                    .entrySet()
                    .stream()
                    .map(s -> name(dataFile(new FileVersion(s.getKey(), s.getValue().version()))));
            return
                    Stream.concat(filenames,
                                  invokeOnNext(n -> n.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp),
                                               Stream.empty()));
        }
        return invokeOnNext(n -> n.getBackupFilenames(lastSegmentBackedUp, lastVersionBackedUp), Stream.empty());
    }

    @Override
    public SortedSet<Long> getSegments() {
        return readBuffers.keySet();
    }

    public long getFirstCompletedSegment() {
        return invokeOnNext(n -> n.allSegments().findFirst().orElse(-1L), -1L);
    }


    @Override
    public Optional<EventSource> eventSource(FileVersion segment) {
        Optional<EventSource> eventSource = localEventSource(segment);
        if (eventSource.isPresent()) {
            return eventSource;
        }
        return invokeOnNext(nextStorageTier -> nextStorageTier.eventSource(segment), Optional.empty());
    }

    @Override
    public Optional<EventSource> eventSource(long segment) {
        Optional<EventSource> eventSource = localEventSource(segment);
        if (eventSource.isPresent()) {
            return eventSource;
        }
        return invokeOnNext(nextStorageTier -> nextStorageTier.eventSource(segment), Optional.empty());
    }

    @Override
    public void close(boolean deleteData) {
        File storageDir = new File(storagePath);
        fileSystemMonitor.unregisterPath(storeName());

        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> {
            source.clean(0);
            if (deleteData) {
                removeSegmentVersions(s);
            }
        });

        applyOnNext(n -> n.close(deleteData));

        indexManager.cleanup(deleteData);
        if (deleteData) {
            FileUtils.delete(storageDir);
        }
    }

    protected Optional<EventSource> localEventSource(FileVersion segment) {
        return localEventSource(segment.segment());
    }

    protected Optional<EventSource> localEventSource(long segment) {
        if (readBuffers.containsKey(segment)) {
            return Optional.of(readBuffers.get(segment).duplicate());
        }
        return Optional.empty();
    }

    @Override
    public void initSegments(long first) {
        // no-op
    }

    @Override
    public void handover(Segment segment, Runnable callback) {
        callback.run();
    }

    @Override
    public boolean removeSegment(long segment, int segmentVersion) {
        if (readBuffers.containsKey(segment)) {
            return removeLocalSegment(segment, segmentVersion);
        }
        return invokeOnNext(n -> n.removeSegment(segment, segmentVersion), true);
    }

    private void removeSegmentVersions(long segment) {
        ByteBufferEventSource eventSource = readBuffers.remove(segment);
        int currentVersion = 0;
        if (eventSource != null) {
            currentVersion = eventSource.version();
            eventSource.clean(0);
        }
        versions(segment, currentVersion).forEach(v -> removeLocalSegment(segment, v));
    }

    private boolean removeLocalSegment(long segment, int version) {
        ByteBufferEventSource eventSource = readBuffers.remove(segment);
        if (eventSource != null) {
            eventSource.clean(0);
        }

        FileVersion fileVersion = new FileVersion(segment, version);
        return indexManager.remove(fileVersion) &&
                FileUtils.delete(dataFile(fileVersion));
    }


    @Override
    public Integer currentSegmentVersion(Long segment) {
        ByteBufferEventSource readBuffer = readBuffers.get(segment);
        if (readBuffer != null) {
            return readBuffer.version();
        }
        return invokeOnNext(n -> n.currentSegmentVersion(segment), 0);
    }

    @Override
    public void activateSegmentVersion(long segment, int segmentVersion) {
        applyOnNext(n -> n.activateSegmentVersion(segment, segmentVersion));
    }
}
