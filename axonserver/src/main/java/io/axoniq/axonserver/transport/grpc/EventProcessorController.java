package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceImplBase;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
@Controller
public class EventProcessorController extends EventProcessorAdminServiceImplBase implements AxonServerClientService {

    private final EventProcessorAdminService service;
    private final AuthenticationProvider authenticationProvider;
    private final EventProcessorStateMapper eventProcessorStateMapper = new EventProcessorStateMapper();

    public EventProcessorController(EventProcessorAdminService service, AuthenticationProvider authenticationProvider) {
        this.service = service;
        this.authenticationProvider = authenticationProvider;
    }

    @Override
    public void pauseEventProcessor(EventProcessorIdentifier request, StreamObserver<Empty> responseObserver) {
        EventProcessorId eventProcessorId = new EventProcessorIdMessage(request);
        Authentication authentication = new GrpcAuthentication(authenticationProvider);
        try {
            service.pause(eventProcessorId, authentication);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void eventProcessors(Empty request, StreamObserver<EventProcessor> r) {
        Authentication authentication = new GrpcAuthentication(authenticationProvider);
        service.eventProcessors(authentication)
               .map(eventProcessorStateMapper)
               .subscribe(r::onNext, r::onError, r::onCompleted);
    }
}
