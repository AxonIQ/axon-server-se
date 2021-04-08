/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.engine.file;

import io.axoniq.axonserver.refactoring.store.SerializedEvent;

/**
 * @author Marc Gathier
 */
public interface EventSource extends AutoCloseable {

    SerializedEvent readEvent(int position);

    default void close() {
        // no-action
    }

    TransactionIterator createTransactionIterator(long segment, long token, boolean validating);

    EventIterator createEventIterator(long segment, long startToken);
}
