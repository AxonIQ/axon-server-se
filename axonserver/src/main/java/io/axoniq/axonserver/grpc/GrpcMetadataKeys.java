/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.grpc.Context;
import io.grpc.Metadata;
import org.springframework.security.core.Authentication;

/**
 * Defines constants for keys that can be passed in gRPC metadata.
 * @author Marc Gathier
 */
public interface GrpcMetadataKeys {
    Metadata.Key<String> TOKEN_KEY = Metadata.Key.of(AxonServerAccessController.TOKEN_PARAM, Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> AXONDB_TOKEN_KEY = Metadata.Key.of(AxonServerAccessController.AXONDB_TOKEN_PARAM, Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> INTERNAL_TOKEN_KEY = Metadata.Key.of("AxonIQ-InternalToken", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> ERROR_CODE_KEY = Metadata.Key.of("AxonIQ-ErrorCode", Metadata.ASCII_STRING_MARSHALLER);

    Metadata.Key<String> CONTEXT_MD_KEY = Metadata.Key.of("AxonIQ-Context", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> AXONDB_CONTEXT_MD_KEY = Metadata.Key.of("Context", Metadata.ASCII_STRING_MARSHALLER);
    Context.Key<String> CONTEXT_KEY = Context.key("AxonIQ-Context");
    Context.Key<Authentication> PRINCIPAL_CONTEXT_KEY = Context.key(AxonServerAccessController.PRINCIPAL_PARAM);
}
