package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.internal.SynchronizationReplicaInbound;
import io.axoniq.axonserver.grpc.internal.SynchronizationReplicaOutbound;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public interface DataSychronizationServiceInterface {
    StreamObserver<SynchronizationReplicaOutbound> openConnection(
            StreamObserver<SynchronizationReplicaInbound> responseObserver);

}
