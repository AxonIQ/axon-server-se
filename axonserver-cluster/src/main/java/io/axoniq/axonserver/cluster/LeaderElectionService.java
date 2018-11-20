package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderElectionService extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
    private final Map<String,RaftNode> nodePerGroup = new ConcurrentHashMap<>();

    public void addRaftNode(RaftNode raftNode) {
        nodePerGroup.put(raftNode.groupId(), raftNode);
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        RaftNode node = nodePerGroup.get(request.getGroupId());
        if( node != null) {
            RequestVoteResponse response = node.requestVote(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        responseObserver.onError(new MissingNodeForGroupException(request.getGroupId()));
    }
}
