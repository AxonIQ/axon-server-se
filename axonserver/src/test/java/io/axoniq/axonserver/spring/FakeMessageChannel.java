/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.spring;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * Created by Sara Pellegrini on 13/04/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeMessageChannel implements MessageChannel {

    private final boolean success;

    public FakeMessageChannel(boolean success) {
        this.success = success;
    }

    @Override
    public boolean send(Message<?> message) {
        return success;
    }

    @Override
    public boolean send(Message<?> message, long timeout) {
        return success;
    }
}
