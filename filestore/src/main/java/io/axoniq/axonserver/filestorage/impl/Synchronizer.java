/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Thread responsible to close the segment when it got full.
 * Multiple entries can be written to a segment in parallel. An entry is only confirmed when all previous
 * entries are also completely written. This synchronizer maintains a list of pending writes, so that it can confirm
 * them once done.
 * <p>
 * One instance per context
 *
 * @author Marc Gathier
 * @since 4.6.0
 */

public class Synchronizer {

    private final Logger log = LoggerFactory.getLogger(Synchronizer.class);
    private final SortedMap<WritePosition, CompletableFuture<Long>> writePositions = new ConcurrentSkipListMap<>();

    private final ScheduledExecutorService fsync;
    private final String context;
    private final StorageProperties storageProperties;
    private final Consumer<Long> completeSegmentCallback;
    private final AtomicReference<WritePosition> lastCompletedRef = new AtomicReference<>();
    private final ConcurrentSkipListSet<Long> syncAndCloseFile = new ConcurrentSkipListSet<>();
    private final AtomicBoolean updated = new AtomicBoolean();
    private volatile ScheduledFuture<?> forceJob;
    private volatile ScheduledFuture<?> syncJob;

    public Synchronizer(String context, StorageProperties storageProperties,
                        Consumer<Long> completeSegmentCallback) {
        this.context = context;
        this.storageProperties = storageProperties;
        this.completeSegmentCallback = completeSegmentCallback;
        fsync = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName(context + "-synchronizer");
            return t;
        });
    }

    /**
     * Notification that a single write block has finished. Completes all write blocks that can be completed (as all
     * previous blocks are completed). If all blocks for a segment are completed and the next block is in the next
     * segment it schedules the close of the segment.
     */
    public void notifyWritePositions() {
        try {
            for (Iterator<Map.Entry<WritePosition, CompletableFuture<Long>>> iterator = writePositions
                    .entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<WritePosition, CompletableFuture<Long>> writePositionEntry = iterator
                        .next();

                WritePosition writePosition = writePositionEntry.getKey();
                if (writePosition.isComplete() &&
                        writePositionEntry.getValue().complete(writePosition.sequence)) {
                    updated.set(true);
                    if (canSyncAt(writePosition)) {
                        syncAndCloseFile.add(lastCompletedRef.get().segment);
                    }
                    lastCompletedRef.set(writePosition);
                    iterator.remove();
                } else {
                    break;
                }
            }
        } catch (RuntimeException t) {
            writePositions.entrySet().iterator().forEachRemaining(e -> e.getValue().completeExceptionally(t));
            log.error("Caught exception in the synchronizer for {}", context, t);
        }
    }

    private void syncAndCloseFile() {
        try {
            Long toSync;
            synchronized (syncAndCloseFile) {
                while ((toSync = syncAndCloseFile.pollFirst()) != null) {
                    closeFile(toSync);
                }
            }
        } catch (RuntimeException t) {
            log.warn("syncAndClose job failed - {}", t.getMessage(), t);
        }
    }

    private void closeFile(Long toSync) {
        log.debug("Syncing segment and index at {}", toSync);
        try {
            completeSegmentCallback.accept(toSync);
        } catch (Exception e) {
            log.debug("Failed to close file {}", toSync, e);
        }
        log.info("Synced segment and index at {}", toSync);
    }

    /**
     * Registers a write action. The write action is completed when the {@code writePosition} is complete and all
     * previous write actions are completed.
     *
     * @param writePosition the write position for a single write block
     */
    public CompletableFuture<Long> register(WritePosition writePosition) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        writePositions.put(writePosition, completableFuture);
        return completableFuture;
    }


    private boolean canSyncAt(WritePosition nextCompleted) {
        WritePosition lastCompleted = lastCompletedRef.get();
        if (lastCompleted == null) {
            return false;
        }

        if (!Objects.equals(lastCompleted.segment, nextCompleted.segment)) {
            log.debug("can sync at {}: {}", nextCompleted.segment, lastCompleted.segment);
        }
        return !Objects.equals(lastCompleted.segment, nextCompleted.segment);
    }

    /**
     * Initializes the synchronizer and schedules tasks to fsync the current segment and to check for segments to complete.
     * @param writePosition position/segment number where the next entry will be written
     */
    public synchronized void init(WritePosition writePosition) {
        lastCompletedRef.set(writePosition);
        log.debug("Initializing at {}", writePosition);
        if (syncJob == null) {
            syncJob = fsync.scheduleWithFixedDelay(this::syncAndCloseFile,
                                                   storageProperties.getSyncInterval(),
                                                   storageProperties.getSyncInterval(),
                                                   TimeUnit.MILLISECONDS);
            log.debug("Scheduled syncJob");
        }
        if (forceJob == null) {
            forceJob = fsync.scheduleWithFixedDelay(this::forceCurrent,
                                                    storageProperties.getForceInterval(),
                                                    storageProperties.getForceInterval(),
                                                    TimeUnit.MILLISECONDS);
            log.debug("Scheduled forceJob");
        }
    }

    private void forceCurrent() {
        if (updated.compareAndSet(true, false)) {
            if (lastCompletedRef.get() != null) {
                lastCompletedRef.get().force();
            }
        }
    }

    /**
     * Stops the synchronized scheduled tasks. Waits until all pending work has completed.
     * @param shutdown Gracefully shutdown thread executor
     */
    public void shutdown(boolean shutdown) {
        if (syncJob != null) {
            syncJob.cancel(false);
        }
        if (forceJob != null) {
            forceJob.cancel(false);
        }
        syncJob = null;
        forceJob = null;
        waitForPendingWrites();
        syncAndCloseFile();
        if (!writePositions.isEmpty()) {
            writePositions.clear();
        }
        if (shutdown) {
            fsync.shutdown();
        }
        WritePosition lastCompleted = lastCompletedRef.getAndSet(null);
        if( lastCompleted != null) {
            lastCompleted.force();
        }
    }

    /**
     * Confirms that all thread executor is gracefully shutdown and jobs are done
     */
    public boolean isShutdown() {
        return ((syncJob == null || (syncJob.isDone() || syncJob.isCancelled()))
                && (forceJob == null || (forceJob.isDone() || forceJob.isCancelled()))
                && (fsync == null || (fsync.isShutdown() || fsync.isTerminated())));
    }

    private void waitForPendingWrites() {
        int retries = 1000;
        while (!writePositions.isEmpty() && retries > 0) {
            try {
                Thread.sleep(1);
                retries--;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FileStoreException(FileStoreErrorCode.INTERRUPTED, "Interrupted while closing synchronizer");
            }
        }
    }
}
