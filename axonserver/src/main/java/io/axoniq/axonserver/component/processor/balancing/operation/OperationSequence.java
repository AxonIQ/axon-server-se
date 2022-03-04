/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.operation;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;

import java.util.List;

/**
 * Created by Sara Pellegrini on 07/08/2018.
 * sara.pellegrini@gmail.com
 */
public class OperationSequence implements LoadBalancingOperation {

    private final List<LoadBalancingOperation> sequence;

    public OperationSequence(List<LoadBalancingOperation> sequence) {
        this.sequence = sequence;
    }

    @Override
    public void perform() {
        sequence.forEach(LoadBalancingOperation::perform);
    }
}
