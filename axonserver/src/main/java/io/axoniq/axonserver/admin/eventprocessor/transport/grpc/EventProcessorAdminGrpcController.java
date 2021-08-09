package io.axoniq.axonserver.admin.eventprocessor.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.admin.ContextId;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceImplBase;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
public class EventProcessorAdminGrpcController extends EventProcessorAdminServiceImplBase {

    private final EventProcessorAdminService service;
    private final AuthenticationProvider authenticationProvider;

    public EventProcessorAdminGrpcController(EventProcessorAdminService service,
                                             @Qualifier("GrpcContextAuthenticationProvider")
                                                     AuthenticationProvider authenticationProvider) {
        this.service = service;
        this.authenticationProvider = authenticationProvider;
    }

    @Override
    public void pauseEventProcessor(EventProcessorIdentifier id, StreamObserver<Empty> responseObserver) {
        EventProcessorId eventProcessorId = new EventProcessorId(id.getProcessorName(),
                                                                 id.getContext(),
                                                                 id.getTokenStoreIdentifier());
        Authentication authentication = new Authentication(authenticationProvider.get().getName());
        service.pause(eventProcessorId, authentication);
        responseObserver.onCompleted();
    }

    @Override
    public void eventProcessorsPerContext(ContextId context, StreamObserver<EventProcessor> r) {
        Authentication authentication = new Authentication(authenticationProvider.get().getName());
        service.eventProcessorsForContext(context.getName(), authentication)
               .map(processor -> EventProcessor.newBuilder().build())
               .subscribe(r::onNext, r::onError, r::onCompleted);
    }
}
