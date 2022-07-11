/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Stream;

public interface SegmentGroup {

    void handover(Segment segment, Runnable callback);

    SortedSet<Long> getSegments();

    long getFirstToken();

    long getTokenAt(long instant);

    long getSegmentFor(long token);

    Stream<String> getBackupFilenames(long lastSegmentBackedUp);

    Optional<EventSource> eventSource(long segment);

    void initSegments(long l);

    void close(boolean deleteData);

    long getFirstCompletedSegment();

    Stream<Long> getAllSegments();
}
