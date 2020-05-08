package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marc Gathier
 */
public class DefaultGrpcRaftClientFactory implements GrpcRaftClientFactory {

    private final Map<String, Channel> channelMap = new ConcurrentHashMap<>();


    @Override
    public LogReplicationServiceGrpc.LogReplicationServiceStub createLogReplicationServiceStub(Node node) {
        return LogReplicationServiceGrpc.newStub(getChannel(node));
    }

    private Channel getChannel(Node node) {
        return channelMap.computeIfAbsent(node.getNodeId(), n -> NettyChannelBuilder.forAddress(node.getHost(),
                                                                                                node.getPort())
                                                                                    .usePlaintext().build());
    }

    @Override
    public LeaderElectionServiceGrpc.LeaderElectionServiceStub createLeaderElectionStub(Node node) {
        return LeaderElectionServiceGrpc.newStub(getChannel(node));
    }
}
