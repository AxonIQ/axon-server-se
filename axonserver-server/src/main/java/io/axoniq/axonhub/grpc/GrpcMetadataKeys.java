package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.AxonHubAccessController;
import io.grpc.Context;
import io.grpc.Metadata;

/**
 * Author: marc
 */
public interface GrpcMetadataKeys {
    Metadata.Key<String> TOKEN_KEY = Metadata.Key.of(AxonHubAccessController.TOKEN_PARAM, Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> INTERNAL_TOKEN_KEY = Metadata.Key.of("AxonIQ-InternalToken", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> ERROR_CODE_KEY = Metadata.Key.of("AxonIQ-ErrorCode", Metadata.ASCII_STRING_MARSHALLER);

    Metadata.Key<String> CONTEXT_MD_KEY = Metadata.Key.of("AxonIQ-Context", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> AXONDB_CONTEXT_MD_KEY = Metadata.Key.of("Context", Metadata.ASCII_STRING_MARSHALLER);
    Context.Key<String> CONTEXT_KEY = Context.key("AxonIQ-Context");
    Context.Key<String> TOKEN_CONTEXT_KEY = Context.key(AxonHubAccessController.TOKEN_PARAM);
}
