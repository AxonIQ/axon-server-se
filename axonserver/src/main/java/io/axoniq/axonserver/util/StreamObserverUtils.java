/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class StreamObserverUtils {

    public static void complete(StreamObserver<?> responseStreamObserver) {
        try {
            if (responseStreamObserver != null) {
                responseStreamObserver.onCompleted();
            }
        } catch (Exception t) {
            // Ignore errors here
        }
    }

    public static void error(StreamObserver<?> responseStreamObserver, Throwable cause) {
        try {
            if (responseStreamObserver != null) {
                responseStreamObserver.onError(cause);
            }
        } catch (Exception t) {
            // Ignore errors here
        }
    }
}
