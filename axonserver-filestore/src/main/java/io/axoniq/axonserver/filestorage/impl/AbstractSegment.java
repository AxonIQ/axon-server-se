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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public abstract class AbstractSegment {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractSegment.class);

    static final byte VERSION = 0;
    protected final String context;
    protected final StorageProperties storageProperties;
    protected volatile AbstractSegment next;

    public AbstractSegment(String context, StorageProperties storageProperties, AbstractSegment next) {
        this.context = context;
        this.storageProperties = storageProperties;
        this.next = next;
    }


    public abstract void handover(Long segment, Runnable callback);

    public void init(boolean validate) {
        initSegments(Long.MAX_VALUE);
        if (validate) {
            validate(storageProperties.getValidationSegments());
        }
    }

    public void validate(int maxSegments) {
        Stream<Long> segments = getAllSegments();
        List<ValidationResult> resultList = segments.limit(maxSegments).parallel().map(this::validateSegment).collect(
                Collectors.toList());
        resultList.stream().filter(validationResult -> !validationResult.isValid()).findFirst().ifPresent(
                validationResult -> {
                    throw new FileStoreException(FileStoreErrorCode.VALIDATION_FAILED, validationResult.getMessage());
                });
        resultList.sort(Comparator.comparingLong(ValidationResult::getSegment));
        for( int i = 0 ; i < resultList.size() - 1 ; i++) {
            ValidationResult thisResult = resultList.get(i);
            ValidationResult nextResult = resultList.get(i + 1);
            if (thisResult.getLastToken() + 1 != nextResult.getSegment()) {
                logger.warn("{}: Validation exception: segment {} ending at {}",
                            context,
                            thisResult.getSegment(),
                            thisResult.getLastToken());
                return;
            }
        }
    }

    private Stream<Long> getAllSegments() {
        if( next == null) return getSegments().stream();
        return Stream.concat(getSegments().stream(), next.getSegments().stream()).distinct();
    }

    protected ValidationResult validateSegment(long segment) {
        logger.debug("{}: Validating segment: {}", context, segment);
        try (CloseableIterator<FileStoreEntry> iterator = getEntries(segment, segment)) {
            long last = segment - 1;
            while (iterator.hasNext()) {
                iterator.next();
                last++;
            }
            return new ValidationResult(segment, last);
        } catch (Exception ex) {
            return new ValidationResult(segment, ex.getMessage());
        }
    }

    public abstract void initSegments(long lastInitialized);


    protected CloseableIterator<FileStoreEntry> getEntries(long segment, long token) {
        return createIterator(segment, token)
                .orElseGet(() -> next.getEntries(segment, token));
    }

    protected abstract Optional<CloseableIterator<FileStoreEntry>> createIterator(long segment, long startIndex);

    public long getSegmentFor(long token) {
        for (Long segment : getSegments()) {
            if (segment <= token) {
                return segment;
            }
        }
        if (next == null) {
            return -1;
        }
        return next.getSegmentFor(token);
    }

    protected SortedSet<Long> prepareSegmentStore(long lastInitialized) {
        SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
        File logEntryFolder = storageProperties.getStorage();
        FileUtils.checkCreateDirectory(logEntryFolder);
        String[] entriesFiles = FileUtils.getFilesWithSuffix(logEntryFolder, storageProperties.getSuffix());
        Arrays.stream(entriesFiles)
              .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
              .filter(segment -> segment < lastInitialized)
              .forEach(segments::add);

        return segments;
    }

    public abstract void cleanup(int delay);

    /**
     * Get all segments
     * @return descending set of segment ids
     */
    protected abstract SortedSet<Long> getSegments();

    /**
     * Returns an iterator for the entries in the segment containing {@code nextIndex}.
     * Returns {@code null} when no segment for index found.
     *
     * @param nextIndex index of first entry to return
     * @return the iterator
     */
    public CloseableIterator<FileStoreEntry> getSegmentIterator(long nextIndex) {
        long segment = getSegmentFor(nextIndex);
        return createIterator(segment, nextIndex)
                .orElseThrow(() -> new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR, "Failed to create iterator from index: " + nextIndex));
    }

    /**
     * Confirms that store is gracefully closed
     */
    public abstract boolean isClosed();

    /**
     * Gracefully closes all thread executors that are active
     * @param deleteData cleans up and removes store data
     */
    public abstract void close(boolean deleteData);

    protected abstract void removeSegment(long segment);

    protected abstract void reset(long sequence);
}
