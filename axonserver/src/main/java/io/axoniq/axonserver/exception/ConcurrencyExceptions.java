/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Utility class to easily retrieve the real exception in case of an exception thrown in a concurrent call.
 * @author Marc Gathier
 * @since 4.2.2
 */
public class ConcurrencyExceptions {

    private ConcurrencyExceptions() {
    }

    /**
     * Returns the real cause from an exception as thrown by a completable future.
     *
     * @param t throwable to unwrap
     * @return unwrapped throwable
     */
    public static Throwable unwrap(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return unwrap(t.getCause());
        }
        return t;
    }
}
