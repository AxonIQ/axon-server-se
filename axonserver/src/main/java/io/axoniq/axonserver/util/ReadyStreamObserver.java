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
 * Extension of StreamObserver that adds isReady operation (@see {@link io.grpc.stub.CallStreamObserver}).
 * @author Marc Gathier
 * @since 4.1.2
 */
public interface ReadyStreamObserver<T> extends StreamObserver<T> {
    boolean isReady();

}
