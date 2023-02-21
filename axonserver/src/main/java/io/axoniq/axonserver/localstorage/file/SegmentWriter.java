/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.Event;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Writes events to an event store segment file.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface SegmentWriter extends Closeable {

    /**
     * Returns the token of the last written event.
     *
     * @return the token of the last written event
     */
    long lastToken();

    /**
     * Writes any data that is needed at the end of the file.
     *
     * @throws IOException when writing fails
     */
    void writeEndOfFile() throws IOException;

    /**
     * Writes a list of events to the writer.
     *
     * @param events a list of events
     * @throws IOException when writing fails
     */
    void write(List<Event> events) throws IOException;

    /**
     * Returns the index entries created by this writer.
     *
     * @return the index entries created by this writer
     */
    Map<String, List<IndexEntry>> indexEntries();
}
