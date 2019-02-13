package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @author Marc Gathier
 */
public class DefaultGrpcRaftClientFactory implements GrpcRaftClientFactory {

    @Override
    public LogReplicationServiceGrpc.LogReplicationServiceStub createLogReplicationServiceStub(Node node) {
        return LogReplicationServiceGrpc.newStub(NettyChannelBuilder.forAddress(node.getHost(), node.getPort()).usePlaintext().build());
    }

    @Override
    public LeaderElectionServiceGrpc.LeaderElectionServiceStub createLeaderElectionStub(Node node) {
        return LeaderElectionServiceGrpc.newStub(NettyChannelBuilder.forAddress(node.getHost(), node.getPort()).usePlaintext().build());
    }
}
