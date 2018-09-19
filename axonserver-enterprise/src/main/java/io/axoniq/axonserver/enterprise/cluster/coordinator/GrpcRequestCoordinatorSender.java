package io.axoniq.axonserver.enterprise.cluster.coordinator;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc.MessagingClusterServiceStub;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.internal.MessagingClusterServiceGrpc.newStub;

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
