package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;

/**
 * @author Marc Gathier
 */
public interface GrpcRaftClientFactory {

    LogReplicationServiceGrpc.LogReplicationServiceStub createLogReplicationServiceStub(Node node);

    LeaderElectionServiceGrpc.LeaderElectionServiceStub createLeaderElectionStub(Node node);
}
