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
 * @author Marc Gathier
 * @since
 */
public interface SegmentWriter extends Closeable {

    long lastToken();

    void writeEndOfFile() throws IOException;

    void write(List<Event> events) throws IOException;

    Map<String, List<IndexEntry>> indexEntries();
}
