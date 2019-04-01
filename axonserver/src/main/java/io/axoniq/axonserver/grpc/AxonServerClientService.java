/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.grpc.BindableService;

/**
 * Defines a class as a GRPC bindable service. All components implementing this interface are exposed through the {@link Gateway}.
 * @author Marc Gathier
 */
public interface AxonServerClientService extends BindableService {

}
