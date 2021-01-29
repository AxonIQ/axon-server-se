/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.localstorage.Registration;

import java.util.Set;
import java.util.function.BiConsumer;

/**
 * @author Marc Gathier
 */
public interface ExtensionServiceProvider {

    Registration registerExtensionListener(BiConsumer<ExtensionKey, String> listener);

    <T extends Ordered> Set<ServiceWithInfo<T>> getServicesWithInfo(Class<T> clazz);
}
