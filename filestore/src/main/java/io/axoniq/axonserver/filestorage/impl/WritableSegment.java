/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStoreEntry;
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
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class WritableSegment extends AbstractSegment {

    private static final Logger logger = LoggerFactory.getLogger(WritableSegment.class);
    private static final int HEADER_BYTES = 4 + 1; // Entry Size + Version byte
    private static final int TX_CHECKSUM_BYTES = 4;
    private static final int FILE_HEADER_SIZE = 5;
    private static final int FILE_FOOTER_SIZE = 4;
    private static final int MAX_ENTRY_SIZE = Integer.MAX_VALUE - FILE_HEADER_SIZE - FILE_FOOTER_SIZE;

    private final Synchronizer synchronizer;
    private final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    private final AtomicLong lastIndex = new AtomicLong(0);
    private final ConcurrentNavigableMap<Long, Map<Long, Integer>> positionsPerSegmentMap = new ConcurrentSkipListMap<>();
    private final Map<Long, ByteBufferEntrySource> readBuffers = new ConcurrentHashMap<>();
    private final AtomicReference<FileStoreEntry> lastEntry = new AtomicReference<>();

    public WritableSegment(String context,
                           StorageProperties storageProperties,
                           AbstractSegment next
    ) {
        super(context, storageProperties, next);
        lastIndex.set(storageProperties.getFirstIndex() - 1);
        synchronizer = new Synchronizer(context, storageProperties, this::completeSegment);
    }

    @Override
    public void initSegments(long lastInitialized) {
        logger.info("{}: Initializing log", context);
        File storageDir = storageProperties.getStorage();
        FileUtils.checkCreateDirectory(storageDir);
        initLatestSegment(lastInitialized, storageDir);
    }

    public CompletableFuture<Long> write(FileStoreEntry entry) {
        if (entry.bytes().length == 0) {
            throw new FileStoreException(FileStoreErrorCode.VALIDATION_FAILED,
                                         "Cannot store empty log entry");
        }
        return store(claim(entry.bytes().length + HEADER_BYTES + TX_CHECKSUM_BYTES, 1), entry);
    }

    public CompletableFuture<Long> write(List<FileStoreEntry> entries) {
        if (entries.size() == 0) {
            throw new FileStoreException(FileStoreErrorCode.VALIDATION_FAILED, "Provide at least one entry");
        }
        long size = 0;
        for (FileStoreEntry entry : entries) {
            if (entry.bytes().length == 0) {
                throw new FileStoreException(FileStoreErrorCode.VALIDATION_FAILED, "Cannot store empty log entry");
            }
            size += entry.bytes().length + HEADER_BYTES + TX_CHECKSUM_BYTES;
        }

        WritePosition position = claim(size, entries.size());
        return store(position, entries);
    }

    @Override
    public CloseableIterator<FileStoreEntry> getSegmentIterator(long nextIndex) {
        if (nextIndex > lastIndex.get()) {
            return null;
        }
        logger.trace("{}: Create iterator at: {}", context, nextIndex);
        return super.getSegmentIterator(nextIndex);
    }

    public CloseableIterator<FileStoreEntry> getEntryIterator(long nextIndex) {
        return new MultiSegmentIterator(this::getSegmentIterator, this::getLastIndex, nextIndex);
    }


    public CloseableIterator<FileStoreEntry> getEntryIterator(long nextIndex, long toIndex) {
        return new MultiSegmentIterator(this::getSegmentIterator, () -> Math.min(getLastIndex(), toIndex), nextIndex);
    }

    @Override
    protected Optional<CloseableIterator<FileStoreEntry>> createIterator(long segment, long startIndex) {
        ByteBufferEntrySource buffer = readBuffers.get(segment);
        if( buffer == null) {
            return Optional.empty();
        }
        return Optional.of(buffer.createEntryIterator(startIndex));
    }

    @Override
    protected ValidationResult validateSegment(long segment) {
        if (segment > lastIndex.get()) {
            return new ValidationResult(segment, lastIndex.get());
        }
        return super.validateSegment(segment);
    }

    private void initLatestSegment(long lastInitialized, File storageDir) {
        long first = getFirstFile(lastInitialized, storageDir);
        WritableEntrySource buffer = getOrOpenDatafile(first, storageProperties.getSegmentSize(), false);
        long sequence = first;
        try (CloseableIterator<FileStoreEntry> iterator = new BufferEntryIterator(buffer, first, first)) {
            Map<Long, Integer> indexPositions = new ConcurrentHashMap<>();
            positionsPerSegmentMap.put(first, indexPositions);
            while (iterator.hasNext()) {
                lastEntry.set(iterator.next());
                sequence++;
            }

            lastIndex.set(sequence - 1);
        }

        buffer.putInt(buffer.position(), 0);

        WritePosition writePosition = new WritePosition(sequence, buffer.position(), buffer, first);
        writePositionRef.set(writePosition);

        if (next != null) {
            next.initSegments(first);
        }
        synchronizer.init(writePosition);
    }

    private long getFirstFile(long lastInitialized, File events) {
        String[] entrylogFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getSuffix());

        return Arrays.stream(entrylogFiles)
                     .map(this::getSegment)
                     .filter(segment -> segment < lastInitialized)
                     .max(Long::compareTo)
                     .orElse(lastIndex.get() + 1);
    }

    private CompletableFuture<Long> store(WritePosition claim, List<FileStoreEntry> entries) {
        logger.trace("{}: Storing {} entries", context, entries.size());
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            synchronizer.register(claim).whenComplete((index, ex) -> {
                if (ex == null) {
                    positionsPerSegmentMap.get(claim.segment).put(claim.sequence,
                                                                  claim.position);

                    lastIndex.set(index);
                    lastEntry.set(entries.get(entries.size()-1));
                    completableFuture.complete(index);
                } else {
                    completableFuture.completeExceptionally(ex);
                }
            });
            write(claim, entries);
            synchronizer.notifyWritePositions();
        } catch (RuntimeException cause) {
            completableFuture.completeExceptionally(cause);
        }

        return completableFuture;
    }


    private CompletableFuture<Long> store(WritePosition claim, FileStoreEntry entry) {
        logger.trace("{}: Storing entry with {} bytes", context, entry.bytes().length);
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            synchronizer.register(claim).whenComplete((index, ex) -> {
                if (ex == null) {
                    positionsPerSegmentMap.get(claim.segment).put(claim.sequence,
                                                                  claim.position);

                    lastIndex.set(index);
                    lastEntry.set(entry);
                    completableFuture.complete(index);
                } else {
                    completableFuture.completeExceptionally(ex);
                }
            });
            write(claim, entry);
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
    public void cleanup(int delay) {
        synchronizer.shutdown(false);
        readBuffers.forEach((s, source) -> source.clean(delay));
        if (next != null) {
            next.cleanup(delay);
        }
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return positionsPerSegmentMap.descendingKeySet();
    }

    public long getLastIndex() {
        return lastIndex.get();
    }

    protected void removeSegment(long segment) {
        logger.info("{}: Removing {} segment", context, segment);
        positionsPerSegmentMap.remove(segment);
        ByteBufferEntrySource eventSource = readBuffers.remove(segment);
        if (eventSource != null) {
            eventSource.clean(0);
        }
        FileUtils.delete(storageProperties.dataFile(segment));
    }

    private void completeSegment(Long segment) {
        try {
            logger.debug("{}: Completing segment {}", context, segment);
        } catch (RuntimeException re) {
            logger.warn("{}: Failed to create index", context, re);
        }
        if (next != null) {
            next.handover(segment, () -> {
                positionsPerSegmentMap.remove(segment);
                ByteBufferEntrySource source = readBuffers.remove(segment);
                logger.debug("{}: Handed over {}, remaining segments: {}",
                             context,
                             segment,
                             positionsPerSegmentMap.keySet());
                source.clean(storageProperties.getPrimaryCleanupDelay());
            });
        }
    }

    private void write(WritePosition writePosition, FileStoreEntry entry) {
        ByteBuffer writeBuffer = writePosition.buffer.duplicate().getBuffer();
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(0);
        writeBuffer.put(entry.version());
        Checksum checksum = new Checksum();
        writeBuffer.put(entry.bytes());
        writeBuffer.putInt(checksum.update(entry.bytes()).get());
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(entry.bytes().length);
    }

    private void write(WritePosition writePosition, List<FileStoreEntry> entries) {
        ByteBuffer writeBuffer = writePosition.buffer.duplicate().getBuffer();
        writeBuffer.position(writePosition.position);
        for (int i = 0; i < entries.size(); i++) {
            FileStoreEntry entry = entries.get(i);
            if (i == 0) {
                writeBuffer.putInt(0);
            } else {
                writeBuffer.putInt(entry.bytes().length);
            }
            writeBuffer.put(entry.version());
            Checksum checksum = new Checksum();
            writeBuffer.put(entry.bytes());
            writeBuffer.putInt(checksum.update(entry.bytes()).get());
        }
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(entries.get(0).bytes().length);
    }

    private WritePosition claim(long size, int count) {
        if (size > MAX_ENTRY_SIZE || size <= 0) {
            throw new FileStoreException(FileStoreErrorCode.PAYLOAD_TOO_LARGE,
                                         String.format("Illegal transaction size: %d", size));
        }
        int totalSize = (int) size;
        WritePosition writePosition;
        do {
            writePosition = writePositionRef.getAndAccumulate(
                    new WritePosition(count, totalSize),
                    (prev, x) -> prev.incrementedWith(x.sequence, x.position));

            if (writePosition.isOverflow(totalSize)) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", context, writePosition.sequence);

                writePosition.buffer.putInt(writePosition.position, -1);

                WritableEntrySource buffer = getOrOpenDatafile(writePosition.sequence,
                                                               (long) totalSize + FILE_HEADER_SIZE + FILE_FOOTER_SIZE,
                                                               true);
                writePositionRef.set(writePosition.reset(buffer));
            }
        } while (!writePosition.isWritable(totalSize));

        return writePosition;
    }

    private WritableEntrySource getOrOpenDatafile(long segment, long minSize, boolean recreate) {
        File file = storageProperties.dataFile(segment);
        long size = Math.max(storageProperties.getSegmentSize(), minSize);
        boolean exists = file.exists();
        if (exists) {
            if (recreate && file.length() < minSize) {
                ByteBufferEntrySource buffer = readBuffers.remove(segment);
                if (buffer != null) {
                    buffer.clean(0);
                }
                logger.debug("{}: File for segment {} already exists with size {}. Recreated with size {}.",
                             context,
                             segment,
                             file.length(),
                             size);
                FileUtils.delete(file);
            } else {
                size = file.length();
                logger.debug("{}: File for segment {} already exists with size {}. Reopening.",
                             context,
                             segment,
                             size);
            }
        } else {
            logger.info("{}: File for segment {} does not exist. Creating new file with size of {}.",
                        context,
                        segment,
                        size);
        }
        try (RandomAccessFile r = new RandomAccessFile(file, "rw");
             FileChannel fileChannel= r.getChannel()) {
            positionsPerSegmentMap.computeIfAbsent(segment, k -> new ConcurrentHashMap<>());
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            int bufferLimit = buffer.limit();
            logger.debug("{}: Opened buffer for segment file {} with limit {}.",
                         context,
                         segment,
                         bufferLimit);
            checkBuffer(segment, size, bufferLimit);
            if (!exists) {
                buffer.put(VERSION);
                buffer.putInt(storageProperties.getFlags());
            } else {
                buffer.position(5);
            }
            WritableEntrySource writableEventSource = new WritableEntrySource(buffer,
                                                                              storageProperties
                                                                                      .isForceClean(),
                                                                              segment);
            readBuffers.put(segment, writableEventSource);
            return writableEventSource;
        } catch (IOException ioException) {
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR,
                                         context + ": Failed to open segment: " + segment,
                                         ioException);
        }
    }

    private void checkBuffer(long segment, long size, int bufferLimit) {
        if (bufferLimit != size) {
            logger.warn(
                    "{}: Buffer limit of {} and segment size of {} do not match. Did you change segment size in storage properties?",
                    context,
                    bufferLimit,
                    size);
        }
        if (bufferLimit == 0) {
            String message =
                    context + ": Segment file " + segment + " has 0 buffer limit and size of " + size
                            + ". It looks like it's corrupted. Aborting.";
            logger.error(message);
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR, message);
        }
    }

    public void clear(long lastIndex) {
        logger.info("{}: Clearing log entries, setting last index to {}", context, lastIndex);
        if (next != null) {
            next.getSegments().forEach(segment -> next.removeSegment(segment));
        }
        getSegments().forEach(this::removeSegment);
        cleanup(0);
        positionsPerSegmentMap.clear();
        this.lastIndex.set(lastIndex);
    }


    public void delete() {
        logger.info("{}: Deleting context.", context);
        clear(0);
        File storageDir = storageProperties.getStorage();
        FileUtils.delete(storageDir);
        synchronizer.shutdown(true);
    }

    private Long getSegment(String segmentName) {
        return Long.valueOf(segmentName.substring(0, segmentName.indexOf('.')));
    }

    @Override
    public boolean isClosed() {
        return synchronizer.isShutdown();
    }

    @Override
    public void close(boolean deleteData) {
        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> {
            source.clean(0);
            if (deleteData) {
                removeSegment(s);
            }
        });

        if (next != null) {
            next.close(deleteData);
        }

        if (deleteData) {
            delete();
        }
    }

    public FileStoreEntry lastEntry() {
        return lastEntry.get();
    }

    public boolean isEmpty() {
        return lastIndex.get() <= storageProperties.getFirstIndex();
    }
}
