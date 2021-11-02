package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceImplBase;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.admin.MoveSegment;
import io.axoniq.axonserver.transport.grpc.eventprocessor.EventProcessorIdMessage;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

/**
 * Exposed through GRPC the operations applicable to an Event Processor.
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
@Controller
public class EventProcessorGrpcController extends EventProcessorAdminServiceImplBase
        implements AxonServerClientService {

    private final EventProcessorAdminService service;
    private final AuthenticationProvider authenticationProvider;

    /**
     * Constructor that specify the service to perform the requested operation and the authentication provider.
     *
     * @param service                used to perform the requested operations
     * @param authenticationProvider used to retrieve the information related to the authenticated user
     */
    public EventProcessorGrpcController(EventProcessorAdminService service,
                                        AuthenticationProvider authenticationProvider) {
        this.service = service;
        this.authenticationProvider = authenticationProvider;
    }

    /**
     * Processes the request to pause a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void pauseEventProcessor(EventProcessorIdentifier processorId, StreamObserver<Empty> responseObserver) {
        service.pause(new EventProcessorIdMessage(processorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, responseObserver::onError, responseObserver::onCompleted);
    }

    /**
     * Processes the request to start a specific event processor.
     *
     * @param eventProcessorId the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void startEventProcessor(EventProcessorIdentifier eventProcessorId, StreamObserver<Empty> responseObserver) {
        service.start(new EventProcessorIdMessage(eventProcessorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, responseObserver::onError, responseObserver::onCompleted);
    }

    /**
     * Processes the request to split the bigger segment of a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void splitEventProcessor(EventProcessorIdentifier processorId, StreamObserver<Empty> responseObserver) {
        service.split(new EventProcessorIdMessage(processorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, responseObserver::onError, responseObserver::onCompleted);
    }

    /**
     * Processes the request to split the bigger segment of a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void mergeEventProcessor(EventProcessorIdentifier processorId, StreamObserver<Empty> responseObserver) {
        service.merge(new EventProcessorIdMessage(processorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, responseObserver::onError, responseObserver::onCompleted);
    }

    /**
     * Processes the request to move one segment of a certain event processor to a specific client.
     *
     * @param request          the request details
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void moveEventProcessorSegment(MoveSegment request, StreamObserver<Empty> responseObserver) {
        try {
            service.move(new EventProcessorIdMessage(request.getEventProcessor()),
                         request.getSegment(),
                         request.getTargetClientId(),
                         new GrpcAuthentication(authenticationProvider));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
