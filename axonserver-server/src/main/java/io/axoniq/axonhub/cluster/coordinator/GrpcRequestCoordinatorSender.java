package io.axoniq.axonhub.cluster.coordinator;

import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.grpc.ManagedChannelHelper;
import io.axoniq.axonhub.grpc.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonhub.internal.grpc.MessagingClusterServiceGrpc.MessagingClusterServiceStub;
import io.axoniq.axonhub.internal.grpc.NodeContext;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import static io.axoniq.axonhub.internal.grpc.MessagingClusterServiceGrpc.newStub;

/**
 * Created by Sara Pellegrini on 23/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class GrpcRequestCoordinatorSender implements Sender<NodeContext, ClusterNode, StreamObserver<Confirmation>> {

    private final MessagingPlatformConfiguration configuration;

    public GrpcRequestCoordinatorSender(MessagingPlatformConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void send(NodeContext o, ClusterNode node, StreamObserver<Confirmation> callback) {
            ManagedChannel managedChannel = ManagedChannelHelper.createManagedChannel(configuration,node);
            String internalToken = configuration.getAccesscontrol().getInternalToken();
            InternalTokenAddingInterceptor interceptor = new InternalTokenAddingInterceptor(internalToken);
            MessagingClusterServiceStub stub = newStub(managedChannel).withInterceptors(interceptor);
            stub.requestToBeCoordinator(o, callback);
    }
}
