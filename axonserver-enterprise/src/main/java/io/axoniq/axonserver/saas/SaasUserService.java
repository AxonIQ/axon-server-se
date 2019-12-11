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
import org.springframework.stereotype.Service;

/**
 * @author Marc Gathier
 */
@Service
public class SaasUserService extends SaasUserServiceGrpc.SaasUserServiceImplBase implements AxonServerInternalService {

    private final ContextProvider contextProvider;
    private final ContextStateProvider contextStateProvider;
    private final EventStoreManager eventStoreManager;

    public SaasUserService(ContextProvider contextProvider,
                           ContextStateProvider contextStateProvider,
                           EventStoreManager eventStoreManager) {
        this.contextProvider = contextProvider;
        this.contextStateProvider = contextStateProvider;
        this.eventStoreManager = eventStoreManager;
    }


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

    @Override
    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver) {
        try {
            String context = contextProvider.getContext();
            return eventStoreManager.getEventStore(context).queryEvents(context, responseObserver);
        } catch (Exception ex) {
            responseObserver.onError(ex);
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
