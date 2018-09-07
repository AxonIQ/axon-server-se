package io.axoniq.axonserver.grpc;

import io.axoniq.axonhub.internal.grpc.SynchronizationReplicaInbound;
import io.axoniq.axonhub.internal.grpc.SynchronizationReplicaOutbound;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public interface DataSychronizationServiceInterface {
    StreamObserver<SynchronizationReplicaOutbound> openConnection(
            StreamObserver<SynchronizationReplicaInbound> responseObserver);

}
