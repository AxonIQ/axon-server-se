/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import org.springframework.messaging.simp.stomp.StompHeaderAccessor;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class SubscriptionKey {
    private final String sessionId;
    private final String subscriptionId;
    public SubscriptionKey(StompHeaderAccessor sha) {
        sessionId = sha.getSessionId();
        subscriptionId = sha.getSubscriptionId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionKey that = (SubscriptionKey) o;
        return Objects.equals(sessionId, that.sessionId) &&
                Objects.equals(subscriptionId, that.subscriptionId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(sessionId, subscriptionId);
    }
}
