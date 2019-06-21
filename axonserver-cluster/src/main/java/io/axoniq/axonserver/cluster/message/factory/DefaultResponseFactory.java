package io.axoniq.axonserver.cluster.message.factory;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftResponseFactory;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotSuccess;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.ResponseHeader;

import java.util.UUID;

/**
 * Default implementation of {@link RaftResponseFactory} that uses gRPC builder to create responses.
 *
 * @author Sara Pellegrini
 * @since 4.1.5
 */
public class DefaultResponseFactory implements RaftResponseFactory {

    private final RaftGroup raftGroup;

    public DefaultResponseFactory(RaftGroup raftGroup) {
        this.raftGroup = raftGroup;
    }

    @Override
    public AppendEntriesResponse appendEntriesSuccess(String requestId, long lastLogIndex) {
        return AppendEntriesResponse.newBuilder()
                                    .setResponseHeader(responseHeader(requestId))
                                    .setGroupId(raftGroup.raftConfiguration().groupId())
                                    .setTerm(raftGroup.localElectionStore().currentTerm())
                                    .setSuccess(AppendEntrySuccess.newBuilder().setLastLogIndex(lastLogIndex))
                                    .build();
    }

    @Override
    public AppendEntriesResponse appendEntriesFailure(String requestId, String failureCause) {
        AppendEntryFailure failure = AppendEntryFailure
                .newBuilder()
                .setCause(failureCause)
                .setLastAppliedIndex(raftGroup.logEntryProcessor().lastAppliedIndex())
                .setLastAppliedEventSequence(raftGroup.lastAppliedEventSequence())
                .setLastAppliedSnapshotSequence(raftGroup.lastAppliedSnapshotSequence())
                .build();
        return AppendEntriesResponse
                .newBuilder()
                .setResponseHeader(responseHeader(requestId))
                .setGroupId(raftGroup.raftConfiguration().groupId())
                .setTerm(raftGroup.localElectionStore().currentTerm())
                .setFailure(failure)
                .build();
    }

    @Override
    public InstallSnapshotResponse installSnapshotSuccess(String requestId, int offset) {
        return InstallSnapshotResponse.newBuilder()
                                      .setResponseHeader(responseHeader(requestId))
                                      .setTerm(raftGroup.localElectionStore().currentTerm())
                                      .setGroupId(raftGroup.raftConfiguration().groupId())
                                      .setSuccess(InstallSnapshotSuccess.newBuilder().setLastReceivedOffset(offset))
                                      .build();
    }

    @Override
    public InstallSnapshotResponse installSnapshotFailure(String requestId, String failureCause) {
        return InstallSnapshotResponse.newBuilder()
                                      .setResponseHeader(responseHeader(requestId))
                                      .setGroupId(raftGroup.raftConfiguration().groupId())
                                      .setTerm(raftGroup.localElectionStore().currentTerm())
                                      .setFailure(InstallSnapshotFailure.newBuilder()
                                                                        .setCause(failureCause)
                                                                        .build())
                                      .build();
    }

    @Override
    public RequestVoteResponse voteResponse(String requestId, boolean voteGranted, boolean goAway) {
        return RequestVoteResponse.newBuilder()
                                  .setResponseHeader(responseHeader(requestId))
                                  .setGroupId(raftGroup.raftConfiguration().groupId())
                                  .setVoteGranted(voteGranted)
                                  .setTerm(raftGroup.localElectionStore().currentTerm())
                                  .setGoAway(goAway)
                                  .build();
    }


    private ResponseHeader responseHeader(String requestId) {
        return ResponseHeader.newBuilder()
                             .setRequestId(requestId)
                             .setResponseId(UUID.randomUUID().toString())
                             .setNodeId(raftGroup.localNode().nodeId()).build();
    }
}
