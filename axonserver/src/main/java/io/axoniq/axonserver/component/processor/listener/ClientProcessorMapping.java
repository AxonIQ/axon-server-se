/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface ClientProcessorMapping {

    default ClientProcessor map(String clientId, String component, String context,
                                EventProcessorInfo eventProcessorInfo) {
        return new ClientProcessor() {

            @Override
            public String clientId() {
                return clientId;
            }

            @Override
            public String context() {
                return context;
            }

            @Override
            public EventProcessorInfo eventProcessorInfo() {
                return eventProcessorInfo;
            }

            @Override
            public Boolean belongsToComponent(String c) {
                return c.equals(component);
            }

            @Override
            public boolean belongsToContext(String c) {
                return c.equals(context);
            }
        };
    }

}
