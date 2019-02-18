package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.grpc.cluster.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogReplicationService extends LogReplicationServiceGrpc.LogReplicationServiceImplBase {
    private final static Logger logger = LoggerFactory.getLogger(LogReplicationService.class);
    private final RaftGroupManager raftGroupManager;

    public LogReplicationService(RaftGroupManager raftGroupManager) {
        this.raftGroupManager = raftGroupManager;
    }


    @Override
    public StreamObserver<AppendEntriesRequest> appendEntries(StreamObserver<AppendEntriesResponse> responseObserver) {
        return new StreamObserver<AppendEntriesRequest>() {
            volatile  boolean running = true;
            @Override
            public void onNext(AppendEntriesRequest appendEntriesRequest) {
                if( ! running) return;
                RaftNode target = raftGroupManager.getOrCreateRaftNode(appendEntriesRequest.getGroupId(), appendEntriesRequest.getTargetId());
                try {
                    synchronized (target) {
                        AppendEntriesResponse response = target.appendEntries(appendEntriesRequest);
                        responseObserver.onNext(response);
                        if (response.hasFailure()) {
                            responseObserver.onCompleted();
                            running = false;
                        }
                    }
                } catch( RuntimeException ex) {
                    logger.warn("Failed to process request {}", appendEntriesRequest, ex);
                    responseObserver.onError(ex);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.trace("Failure on appendEntries on leader connection- {}", throwable.getMessage());

            }

            @Override
            public void onCompleted() {
                logger.debug("Connection completed by peer");
            }
        };
    }

    @Override
    public StreamObserver<InstallSnapshotRequest> installSnapshot(StreamObserver<InstallSnapshotResponse> responseObserver) {
        return new StreamObserver<InstallSnapshotRequest>() {
            volatile  boolean running = true;
            @Override
            public void onNext(InstallSnapshotRequest installSnapshotRequest) {
                if( ! running) return;
                RaftNode target = raftGroupManager.raftNode(installSnapshotRequest.getGroupId());
                try {
                    synchronized (target) {
                        InstallSnapshotResponse response = target.installSnapshot(installSnapshotRequest);
                        responseObserver.onNext(response);
                        if (response.hasFailure()) {
                            responseObserver.onCompleted();
                            running = false;
                        }
                    }
                } catch( RuntimeException ex) {
                    logger.warn("Failed to process request {}", installSnapshotRequest, ex);
                    responseObserver.onError(ex);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.trace("Failure on appendEntries on leader connection- {}", throwable.getMessage());

            }

            @Override
            public void onCompleted() {
                logger.debug("Connection completed by peer");
            }
        };
    }
}
