/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.component.ComponentItem;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface Client extends Printable, ComponentItem {

    String name();

    String context();

    default String axonServerNode() {
        return null;
    }

    @Override
    default boolean belongsToContext(String context) {
        return context.equals(context());
    }

    @Override
    default void printOn(Media media) {
        media.with("name", name());
    }
}
