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
import org.springframework.data.util.CloseableIterator;

import java.io.File;
import java.util.Comparator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ReadOnlySegments extends AbstractSegment {

    private final ScheduledExecutorService scheduledExecutorService;
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());

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
    protected Optional<CloseableIterator<FileStoreEntry>> createIterator(long segment, long startIndex) {
        if (!segments.contains(segment)) {
            return Optional.empty();
        }

        return Optional.of(new ReaderEventIterator(storageProperties.dataFile(segment),
                                                   segment,
                                                   startIndex));
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
    }

    protected void removeSegment(long segment) {
        if (segments.remove(segment)) {
            deleteFiles(segment);
        }
    }

    protected void reset(long sequence) {
        for (long segment : getSegments()) {
            if (segment > sequence) {
                removeSegment(segment);
            }
        }

        if (segments.isEmpty() && next != null) {
            next.reset(sequence);
        }
    }

    @Override
    public boolean isClosed() {
        return scheduledExecutorService.isShutdown() || scheduledExecutorService.isTerminated();
    }

    @Override
    public void close(boolean deleteData) {
        scheduledExecutorService.shutdown();
        if (next != null) next.close(deleteData);
        if (deleteData) {
            segments.forEach(this::removeSegment);
            segments.clear();
        }

    }
}
