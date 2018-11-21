package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.grpc.cluster.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogReplicationService extends LogReplicationServiceGrpc.LogReplicationServiceImplBase {
    private final static Logger logger = LoggerFactory.getLogger(LogReplicationService.class);

    private final Map<String, RaftNode> nodePerGroup = new ConcurrentHashMap<>();

    public void addRaftNode(RaftNode raftNode) {
        nodePerGroup.put(raftNode.groupId(), raftNode);
    }

    @Override
    public StreamObserver<AppendEntriesRequest> appendEntries(StreamObserver<AppendEntriesResponse> responseObserver) {
        return new StreamObserver<AppendEntriesRequest>() {
            @Override
            public void onNext(AppendEntriesRequest appendEntriesRequest) {
                RaftNode target = nodePerGroup.get(appendEntriesRequest.getGroupId());
                AppendEntriesResponse response = target.appendEntries(appendEntriesRequest);
                try {
                    responseObserver.onNext(response);
                    if (response.hasFailure()) {
                        responseObserver.onCompleted();
                    }
                } catch( Exception ex) {
                    // ignore
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("Failure on appendEntries connection", throwable);

            }

            @Override
            public void onCompleted() {
                logger.debug("Connection completed by peer");

            }
        };
    }

    @Override
    public StreamObserver<InstallSnapshotRequest> installSnapshot(StreamObserver<InstallSnapshotResponse> responseObserver) {
        return super.installSnapshot(responseObserver);
    }
}
