/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.api;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Fake implementation of {@link EventProcessorSegment} for test purpose.
 *
 * @author Sara Pellegrini
 */
public class FakeEventProcessorSegment implements EventProcessorSegment {

    private final int id;
    private final int onePartOf;
    private final boolean replaying;
    private final boolean caughtUp;
    private final String claimedBy;

    public FakeEventProcessorSegment(int id) {
        this(id, 1);
    }

    public FakeEventProcessorSegment(int id, int onePartOf) {
        this(id, onePartOf, false);
    }

    public FakeEventProcessorSegment(int id, int onePartOf, boolean replaying) {
        this(id, onePartOf, replaying, false);
    }

    public FakeEventProcessorSegment(int id, int onePartOf, boolean replaying, boolean caughtUp) {
        this(id, onePartOf, replaying, caughtUp, "fakeClient");
    }

    public FakeEventProcessorSegment(int id, int onePartOf, boolean replaying, boolean caughtUp, String claimedBy) {
        this.id = id;
        this.onePartOf = onePartOf;
        this.replaying = replaying;
        this.caughtUp = caughtUp;
        this.claimedBy = claimedBy;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int onePartOf() {
        return onePartOf;
    }

    @Nonnull
    @Override
    public String claimedBy() {
        return claimedBy;
    }

    @Override
    public boolean isCaughtUp() {
        return caughtUp;
    }

    @Override
    public boolean isReplaying() {
        return replaying;
    }

    @Override
    public long tokenPosition() {
        return 0;
    }

    @Override
    public boolean isInError() {
        return false;
    }

    @Nonnull
    @Override
    public Optional<String> error() {
        return Optional.empty();
    }
}
