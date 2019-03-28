/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.serializer.Printable;

import java.util.Set;

/**
 * Author: marc
 */
public interface LoadBalanceStrategyHolder {

    LoadBalancingStrategy findByName(String strategyName);

    Iterable<? extends Printable> findAll();

    Set<String> getFactoryBeans();
}
