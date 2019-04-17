/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class FilePreparedTransaction extends PreparedTransaction {

    private final WritePosition writePosition;
    private final int eventSize;

    public FilePreparedTransaction(WritePosition writePosition, int eventSize, List<ProcessedEvent> eventList) {
        super( writePosition.sequence, eventList);
        this.writePosition = writePosition;
        this.eventSize = eventSize;
    }

    public WritePosition getWritePosition() {
        return writePosition;
    }

    public int getEventSize() {
        return eventSize;
    }
}
