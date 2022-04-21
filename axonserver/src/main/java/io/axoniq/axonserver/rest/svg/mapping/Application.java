/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Application {

    String name();

    String component();

    Iterable<String> contexts();

    int instances();

    Iterable<String> connectedHubNodes();

    default String instancesString(){
        return instances() == 1 ? "1 instance" : instances() + " instances";
    }
}
