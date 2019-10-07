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
 * Utility class to do a safe onComplete/onError on a response stream.
 * @author Marc Gathier
 */
public class StreamObserverUtils {

    /**
     * Completes a gRPC stream, ignoring any errors that may occur during the complete.
     *
     * @param responseStreamObserver the streamObserver to complete
     */
    public static void complete(StreamObserver<?> responseStreamObserver) {
        try {
            if (responseStreamObserver != null) {
                responseStreamObserver.onCompleted();
            }
        } catch (Exception t) {
            // Ignore errors here, stream may already have been cancelled/closed before
        }
    }

    /**
     * Errors a gRPC stream, ignoring any errors that may occur during the sending of the error.
     * @param responseStreamObserver the streamObserver to error
     * @param cause the reason why the stream should send an error
     */
    public static void error(StreamObserver<?> responseStreamObserver, Throwable cause) {
        try {
            if (responseStreamObserver != null) {
                responseStreamObserver.onError(cause);
            }
        } catch (Exception t) {
            // Ignore errors here, stream may already be cancelled
        }
    }
}
