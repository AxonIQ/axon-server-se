/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import org.springframework.data.util.CloseableIterator;

/**
 * @author Marc Gathier
 */
public interface TransactionIterator extends CloseableIterator<SerializedTransactionWithToken> {

    @Override
    default void close() {
        // Default no action, defined here to avoid IOException
    }
}
