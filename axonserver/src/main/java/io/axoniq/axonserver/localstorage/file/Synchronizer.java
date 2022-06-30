/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;


import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.StorageCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
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
 * Thread responsible to close the segment when it got full. Also confirms to writer when transaction blocks are close.
 * One instance per event-type (Event,Snapshot) per context
 *
 * @author Zoltan Altfatter
 */

public class Synchronizer {

    private final Logger log = LoggerFactory.getLogger(Synchronizer.class);
    private final SortedMap<WritePosition, StorageCallback> writePositions = new ConcurrentSkipListMap<>();

    private final ScheduledExecutorService fsync;
    private final EventTypeContext context;
    private final StorageProperties storageProperties;
    private final Consumer<WritePosition> completeSegmentCallback;
    private final AtomicReference<WritePosition> currentRef = new AtomicReference<>();
    private final ConcurrentSkipListSet<WritePosition> syncAndCloseFile = new ConcurrentSkipListSet<>();
    private final AtomicBoolean updated = new AtomicBoolean();
    private volatile ScheduledFuture<?> forceJob;
    private volatile ScheduledFuture<?> syncJob;

    public Synchronizer(EventTypeContext context, StorageProperties storageProperties,
                        Consumer<WritePosition> completeSegmentCallback) {
        this.context = context;
        this.storageProperties = storageProperties;
        this.completeSegmentCallback = completeSegmentCallback;
        fsync = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory(context + "-synchronizer-"));
    }

    public void notifyWritePositions() {
        try {
            boolean removed = false;
            for (Iterator<Map.Entry<WritePosition, StorageCallback>> iterator = writePositions
                    .entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<WritePosition, StorageCallback> writePositionEntry = iterator
                        .next();

                WritePosition current = currentRef.get();

                WritePosition writePosition = writePositionEntry.getKey();
                if (!writePosition.isComplete()) {
                    break;
                }

                if (writePosition.sequence > current.sequence + writePosition.prevEntries) {
                    break;
                }

                if (!writePositionEntry.getValue().complete(writePosition.sequence)) {
                    break;
                }
                updated.set(true);

                if (canSyncAt(writePosition, current)) {
                    syncAndCloseFile.add(current);
                }
                removed = true;
                currentRef.updateAndGet(old -> old.sequence < writePosition.sequence ? writePosition : old);
                iterator.remove();
            }
            if (removed) {
                fsync.execute(this::notifyWritePositions);
            }
        } catch (RuntimeException t) {
            writePositions.entrySet().iterator().forEachRemaining(e -> e.getValue().error(t));
            log.error("Caught exception in the synchronizer for {}", context, t);
        }
    }

    private boolean syncAndCloseFile() {
        WritePosition toSync = syncAndCloseFile.pollFirst();
        if (toSync != null) {
            try {
                log.debug("Syncing segment and index at {}", toSync);
                completeSegmentCallback.accept(toSync);
                log.info("Synced segment and index at {}", toSync);
            } catch (Exception ex) {
                log.warn("Failed to close file {} - {}", toSync.segment, ex.getMessage());
                syncAndCloseFile.add(toSync);
                return false;
            }
        }
        return true;
    }

    public void register(WritePosition writePosition, StorageCallback callback) {
        writePositions.put(writePosition, callback);
    }


    private boolean canSyncAt(WritePosition writePosition, WritePosition current) {
        if (current == null) {
            return false;
        }
        if (!Objects.equals(current.segment, writePosition.segment)) {
            log.debug("can sync at {}: {}", writePosition.segment, current.segment);
        }
        return !Objects.equals(current.segment, writePosition.segment);
    }

    public synchronized void init(WritePosition writePosition) {
        currentRef.set(writePosition);
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

    public void forceCurrent() {
        if (updated.compareAndSet(true, false)) {
            WritePosition writePosition = currentRef.get();
            if (writePosition != null) {
                writePosition.force();
            }
        }
    }

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
        boolean closeMore = true;
        while (closeMore && !syncAndCloseFile.isEmpty()) {
            closeMore = syncAndCloseFile();
        }
        if (shutdown) {
            fsync.shutdown();
        }
        WritePosition writePosition = currentRef.getAndSet(null);
        if (writePosition != null) {
            writePosition.force();
        }
    }

    private void waitForPendingWrites() {
        int retries = 1000;
        while (!writePositions.isEmpty() && retries > 0) {
            try {
                Thread.sleep(1);
                retries--;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MessagingPlatformException(ErrorCode.INTERRUPTED, "Interrupted while closing synchronizer");
            }
        }
    }
}
