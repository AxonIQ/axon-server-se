package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.MissingNodeForGroupException;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.grpc.stub.StreamObserver;

public class LeaderElectionService extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
    private final RaftGroupManager raftGroupManager;

    public LeaderElectionService(RaftGroupManager raftGroupManager) {
        this.raftGroupManager = raftGroupManager;
    }


    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        RaftNode node = raftGroupManager.raftNode(request.getGroupId());
        if( node != null) {
            try {
                RequestVoteResponse response = node.requestVote(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        } else {
            responseObserver.onError(new MissingNodeForGroupException(request.getGroupId()));
        }
    }

    @Override
    public void requestPreVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        RaftNode node = raftGroupManager.raftNode(request.getGroupId());
        if (node != null) {
            try {
                RequestVoteResponse response = node.requestPreVote(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        } else {
            responseObserver.onError(new MissingNodeForGroupException(request.getGroupId()));
        }
    }
}
