/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class ReadOnlySegments extends AbstractSegment {

    private final ScheduledExecutorService scheduledExecutorService;
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final ConcurrentSkipListMap<Long, WeakReference<ByteBufferEntrySource>> lruMap = new ConcurrentSkipListMap<>();


    public ReadOnlySegments(String context,
                            StorageProperties storageProperties) {
        super(context, storageProperties, null);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r);
                    t.setName(context + "-file-cleanup");
                    return t;
                });
    }


    @Override
    public void initSegments(long lastInitialized) {
        segments.addAll(prepareSegmentStore(lastInitialized));
        if (next != null) {
            next.initSegments(segments.isEmpty() ? lastInitialized : segments.last());
        }
    }


    @Override
    protected Optional<EntrySource> getEventSource(long segment) {
        return Optional.ofNullable(get(segment, false));
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        segments.add(segment);
        callback.run();
    }

    private void deleteFiles(Long s) {
        logger.debug("{}: Deleting files for segment {}", context, s);
        File datafile = storageProperties.dataFile(s);
        boolean success = FileUtils.delete(datafile);

        if (!success) {
            logger.warn("{}: Deleting files for segment {} not complete, rescheduling", context, s);
            scheduledExecutorService.schedule(() -> deleteFiles(s), 1, TimeUnit.MINUTES);
        }
    }

    @Override
    public void cleanup(int delay) {
        lruMap.forEach((s, source) -> {
            ByteBufferEntrySource eventSource = source.get();
            if (eventSource != null) {
                eventSource.clean(delay);
            }
        });
    }

    protected void removeSegment(long segment) {
        if (segments.remove(segment)) {
            WeakReference<ByteBufferEntrySource> segmentRef = lruMap.remove(segment);
            if (segmentRef != null) {
                ByteBufferEntrySource eventSource = segmentRef.get();
                if (eventSource != null) {
                    eventSource.clean(0);
                }
            }

            deleteFiles(segment);
        }
    }

    private ByteBufferEntrySource get(long segment, boolean force) {
        if (!segments.contains(segment) && !force) {
            return null;
        }
        WeakReference<ByteBufferEntrySource> bufferRef = lruMap.get(segment);
        if (bufferRef != null) {
            ByteBufferEntrySource b = bufferRef.get();
            if (b != null) {
                return b.duplicate();
            }
        }

        File file = storageProperties.dataFile(segment);
        long size = file.length();

        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            ByteBufferEntrySource eventSource = new ByteBufferEntrySource(buffer,
                    storageProperties, segment);
            lruMap.put(segment, new WeakReference<>(eventSource));
            return eventSource;
        } catch (IOException ioException) {
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR,
                                                 context + ": Error while opening segment: " + segment,
                                                 ioException);
        }
    }

    @Override
    public boolean isClosed() {
        return scheduledExecutorService.isShutdown() || scheduledExecutorService.isTerminated();
    }

    @Override
    public void close(boolean deleteData) {
        scheduledExecutorService.shutdown();
        lruMap.forEach((s, source) -> {
            if (source.get() != null) {
                source.get().clean(0);
            }
        });

        if (next != null) next.close(deleteData);

        lruMap.clear();
        if (deleteData) {
            segments.forEach(this::removeSegment);
            segments.clear();
        }

    }
}
