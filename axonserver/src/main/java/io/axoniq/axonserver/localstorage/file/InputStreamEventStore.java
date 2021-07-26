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
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.MeterFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Manages the completed segments for the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class InputStreamEventStore extends SegmentBasedEventStore implements ReadOnlySegmentsHandler {

    private final NavigableMap<Long, Integer> segments = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamEventStore(EventTypeContext context, IndexManager indexManager,
                                 EventTransformerFactory eventTransformerFactory,
                                 StorageProperties storageProperties, MeterFactory meterFactory) {
        super(context, indexManager, storageProperties, meterFactory);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    public void handover(FileVersion segment, Runnable callback) {
        segments.put(segment.segment(), segment.version());
        callback.run();
    }

    @Override
    protected boolean containsSegment(long segment) {
        return segments.containsKey(segment);
    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        Integer version = segments.get(segment);
        if (version != null) {
            return getEventSource(new FileVersion(segment, version));
        }
        return Optional.empty();
    }

    @Override
    public void initSegments(long lastInitialized) {
        segments.putAll(prepareSegmentStore(lastInitialized));
        if (next != null) {
            next.initSegments(segments.isEmpty() ? lastInitialized : segments.navigableKeySet().last());
        }
    }

    @Override
    public void close(boolean deleteData) {
        if (deleteData) {
            segments.forEach((segment, version) -> removeSegment(segment));
        }
    }

    public void transformContents(UnaryOperator<Event> transformationFunction) {
        NavigableSet<Long> segmentsToTransform = new TreeSet<>(segments.keySet());
        for (Long segment : segmentsToTransform) {
            Integer version = segments.get(segment);
            if (version != null) {
                transformSegment(segment, version, transformationFunction);
            }
        }
    }

    private void transformSegment(long segment, int currentVersion, UnaryOperator<Event> transformationFunction) {
        Map<String, List<IndexEntry>> indexEntriesMap = new HashMap<>();
        AtomicBoolean changed = new AtomicBoolean();
        File dataFile = storageProperties.dataFile(
                context,
                new FileVersion(segment, currentVersion + 1));
        try (TransactionIterator transactionIterator = getTransactions(segment, segment);
            DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(dataFile))) {
            dataOutputStream.write(PrimaryEventStore.VERSION);
            dataOutputStream.writeInt(storageProperties.getFlags());
            int pos = 5;
            long token = segment;
            while (transactionIterator.hasNext()) {
                SerializedTransactionWithToken transaction = transactionIterator
                        .next();
                List<Event> updatedEvents =
                        transaction.getEvents()
                                   .stream()
                                   .map(serializedEvent -> {
                                       Event original = serializedEvent.asEvent();
                                       if (serializedEvent.isSoftDeleted()) return original;
                                       Event transformed = transformationFunction.apply(original);
                                       if (transformed == null) transformed = Event.getDefaultInstance();
                                       if (!original.equals(transformed)) {
                                           changed.set(true);
                                       }
                                       return transformed;
                                   })
                                   .collect(Collectors.toList());

                int eventPosition = pos + 7;
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream eventsBlock = new DataOutputStream(bytes);
                for (Event updatedEvent : updatedEvents) {
                    int size = updatedEvent.getSerializedSize();
                    eventsBlock.writeInt(size);
                    eventsBlock.write(updatedEvent.toByteArray());
                    if (!updatedEvent.getAggregateType().isEmpty()) {
                        indexEntriesMap.computeIfAbsent(updatedEvent.getAggregateIdentifier(), id -> new ArrayList<>())
                                       .add(new IndexEntry(updatedEvent.getAggregateSequenceNumber(), eventPosition, token));
                    }
                    eventPosition += size + 4;
                    token++;
                }

                byte[] eventBytes = bytes.toByteArray();
                dataOutputStream.writeInt(eventBytes.length);
                dataOutputStream.write(TRANSACTION_VERSION);
                dataOutputStream.writeShort(updatedEvents.size());
                dataOutputStream.write(eventBytes);
                Checksum checksum = new Checksum();
                pos += eventBytes.length + 4 + 7;
                dataOutputStream.writeInt(checksum.update(eventBytes).get());
            }
            dataOutputStream.writeInt(0);
            dataOutputStream.writeInt(0);

        } catch (Exception e) {
            FileUtils.delete(dataFile);
            throw new RuntimeException(String.format("%s: transformation of segment %d failed", context, segment), e);
        }
        if( changed.get()) {
            indexManager.createNewVersion(segment, currentVersion + 1, indexEntriesMap);
            segments.put(segment, currentVersion + 1);
            scheduleForDeletion(segment, currentVersion);
        } else {
            FileUtils.delete(dataFile);
        }

    }

    private void removeSegment(long segment) {
        Integer version = segments.remove(segment);
        if (version != null && (!FileUtils.delete(storageProperties.dataFile(context, segment)) ||
                !indexManager.remove(segment))) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Failed to rollback " + getType().getEventType()
                                                         + ", could not remove segment: " + segment);
        }
    }

    @Override
    protected void removeSegment(long segment, int currentVersion) {
        if ( !FileUtils.delete(storageProperties.dataFile(context, new FileVersion(segment, currentVersion))) ||
                !indexManager.remove(new FileVersion(segment, currentVersion))) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR,
                                                 "Failed to rollback " + getType().getEventType()
                                                         + ", could not remove segment: " + segment);
        }
    }

    @Override
    public Optional<EventSource> getEventSource(FileVersion segment) {
        logger.debug("Get eventsource: {}", segment);
        InputStreamEventSource eventSource = get(segment, false);
        logger.trace("result={}", eventSource);
        if (eventSource == null) {
            return Optional.empty();
        }
        return Optional.of(eventSource);
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments.navigableKeySet();
    }

    private InputStreamEventSource get(FileVersion segment, boolean force) {
        if (!force && !segments.containsKey(segment.segment())) {
            return null;
        }

        fileOpenMeter.increment();
        return new InputStreamEventSource(storageProperties.dataFile(context, segment),
                                          eventTransformerFactory);
    }

    @Override
    protected void recreateIndex(FileVersion segment) {
        try (InputStreamEventSource is = get(segment, true);
             EventIterator iterator = createEventIterator(is, segment.segment(), segment.segment())) {
            recreateIndexFromIterator(segment, iterator);
        }
    }
}
