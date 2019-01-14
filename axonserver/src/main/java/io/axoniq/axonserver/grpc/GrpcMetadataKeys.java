package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.grpc.Context;
import io.grpc.Metadata;

/**
 * @author Marc Gathier
 */
public interface GrpcMetadataKeys {
    Metadata.Key<String> TOKEN_KEY = Metadata.Key.of(AxonServerAccessController.TOKEN_PARAM, Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> INTERNAL_TOKEN_KEY = Metadata.Key.of("AxonIQ-InternalToken", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> ERROR_CODE_KEY = Metadata.Key.of("AxonIQ-ErrorCode", Metadata.ASCII_STRING_MARSHALLER);

    Metadata.Key<String> CONTEXT_MD_KEY = Metadata.Key.of("AxonIQ-Context", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> AXONDB_CONTEXT_MD_KEY = Metadata.Key.of("Context", Metadata.ASCII_STRING_MARSHALLER);
    Context.Key<String> CONTEXT_KEY = Context.key("AxonIQ-Context");
    Context.Key<String> TOKEN_CONTEXT_KEY = Context.key(AxonServerAccessController.TOKEN_PARAM);
}
