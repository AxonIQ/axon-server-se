package io.axoniq.axonserver.cluster.message.factory;

import com.google.protobuf.UnknownFieldSet;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftResponseFactory;
import io.axoniq.axonserver.cluster.util.GrpcSignedLongUtils;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotSuccess;
import io.axoniq.axonserver.grpc.cluster.RequestIncrementalData;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.ResponseHeader;
import io.axoniq.axonserver.grpc.cluster.Role;

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
    public AppendEntriesResponse appendEntriesFailure(String requestId, String failureCause,
                                                      boolean supportsReplicationGroup, boolean fatal) {
        AppendEntryFailure.Builder failure = AppendEntryFailure
                .newBuilder()
                .setCause(failureCause)
                .setLastAppliedIndex(raftGroup.logEntryProcessor().lastAppliedIndex())
                .setSupportsReplicationGroups(true)
                .setFatal(fatal);

        // if leader is running in older version we need to add the last event and snapshot token for the context with
        // the same name as the replication group
        if (!supportsReplicationGroup) {
            failure.setUnknownFields(UnknownFieldSet.newBuilder()
                                                    .addField(3,
                                                              GrpcSignedLongUtils.createSignedIntField(raftGroup
                                                                                                               .lastAppliedEventSequence(
                                                                                                                       raftGroup
                                                                                                                               .raftConfiguration()
                                                                                                                               .groupId())))
                                                    .addField(4,
                                                              GrpcSignedLongUtils.createSignedIntField(raftGroup
                                                                                                               .lastAppliedSnapshotSequence(
                                                                                                                       raftGroup
                                                                                                                               .raftConfiguration()
                                                                                                                               .groupId())))
                                                    .build());
        }

        return AppendEntriesResponse
                .newBuilder()
                .setResponseHeader(responseHeader(requestId))
                .setGroupId(raftGroup.raftConfiguration().groupId())
                .setTerm(raftGroup.localElectionStore().currentTerm())
                .setFailure(failure)
                .build();
    }


    @Override
    public InstallSnapshotResponse installSnapshotConfigDone(String requestId, int offset, Role role) {
        return InstallSnapshotResponse.newBuilder()
                                      .setResponseHeader(responseHeader(requestId))
                                      .setTerm(raftGroup.localElectionStore().currentTerm())
                                      .setGroupId(raftGroup.raftConfiguration().groupId())
                                      .setRequestIncrementalData(RequestIncrementalData.newBuilder()
                                                                                       .putAllLastEventTokenPerContext(
                                                                                               raftGroup
                                                                                                       .lastEventTokenPerContext(
                                                                                                               role))
                                                                                       .putAllLastSnapshotTokenPerContext(
                                                                                               raftGroup
                                                                                                       .lastSnapshotTokenPerContext(
                                                                                                               role))
                                                                                       .setLastReceivedOffset(offset))
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
    public RequestVoteResponse voteResponse(String requestId, boolean voteGranted) {
        return RequestVoteResponse.newBuilder()
                                  .setResponseHeader(responseHeader(requestId))
                                  .setGroupId(raftGroup.raftConfiguration().groupId())
                                  .setVoteGranted(voteGranted)
                                  .setTerm(raftGroup.localElectionStore().currentTerm())
                                  .build();
    }


    private ResponseHeader responseHeader(String requestId) {
        return ResponseHeader.newBuilder()
                             .setRequestId(requestId)
                             .setResponseId(UUID.randomUUID().toString())
                             .setNodeId(raftGroup.localNode().nodeId()).build();
    }
}
