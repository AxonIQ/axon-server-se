package io.axoniq.axonserver.saas;

import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.internal.EmptyRequest;
import io.axoniq.axonserver.grpc.internal.saas.ContextOverview;
import io.axoniq.axonserver.grpc.internal.saas.SaasUserServiceGrpc;
import io.axoniq.axonserver.state.ContextStateProvider;
import io.grpc.stub.StreamObserver;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Provides an interface to be used by Axon Cloud Console to access runtime information (connected clients and event data).
 * Only enabled when profile axoniq-cloud-support is enabled
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Service
@Profile("axoniq-cloud-support")
public class SaasUserService extends SaasUserServiceGrpc.SaasUserServiceImplBase implements AxonServerInternalService {

    private final ContextProvider contextProvider;
    private final ContextStateProvider contextStateProvider;
    private final EventStoreManager eventStoreManager;

    /**
     * Constructor for the service.
     *
     * @param contextProvider      function to retrieve the context passed by the caller
     * @param contextStateProvider provider to retrieve information on the currently connected clients for a context
     * @param eventStoreManager    event store manager to retrieve a connection to an event store for a specific context
     */
    public SaasUserService(ContextProvider contextProvider,
                           ContextStateProvider contextStateProvider,
                           EventStoreManager eventStoreManager) {
        this.contextProvider = contextProvider;
        this.contextStateProvider = contextStateProvider;
        this.eventStoreManager = eventStoreManager;
    }


    /**
     * Returns an overview of all connected clients for a context.
     * @param request empty request, as grpc call needs a request message
     * @param responseObserver observer for communicating results
     */
    @Override
    public void getOverview(EmptyRequest request, StreamObserver<ContextOverview> responseObserver) {
        String context = contextProvider.getContext();
        try {
            responseObserver.onNext(contextStateProvider.getCurrentState(context));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
    }

    /**
     * Allows caller to perform ad-hoc queries on the event store
     * @param responseObserver observer for returning results to client
     * @return observer for client to send query and permits
     */
    @Override
    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver) {
        try {
            String context = contextProvider.getContext();
            return eventStoreManager.getEventStore(context).queryEvents(context, responseObserver);
        } catch (Exception ex) {
            responseObserver.onError(ex);
            // Return an dummy observer to avoid exceptions in gRPC
            return new StreamObserver<QueryEventsRequest>() {
                @Override
                public void onNext(QueryEventsRequest queryEventsRequest) {

                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }
    }

    @Override
    public boolean requiresContextInterceptor() {
        return true;
    }
}
