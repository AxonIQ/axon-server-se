package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.MissingNodeForGroupException;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderElectionService extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionService.class);
    private final Map<String, RaftNode> nodePerGroup = new ConcurrentHashMap<>();

    public void addRaftNode(RaftNode raftNode) {
        nodePerGroup.put(raftNode.groupId(), raftNode);
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        RaftNode node = nodePerGroup.get(request.getGroupId());
        if( node != null) {
            try {
                RequestVoteResponse response = node.requestVote(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (IllegalStateException illegalState) {
                responseObserver.onError(illegalState);
            }
        } else {
            responseObserver.onError(new MissingNodeForGroupException(request.getGroupId()));
        }
    }
}
