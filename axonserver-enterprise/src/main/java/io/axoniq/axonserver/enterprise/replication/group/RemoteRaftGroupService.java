package io.axoniq.axonserver.enterprise.replication.group;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.ReplicationGroupMemberConverter;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.DeleteReplicationGroupRequest;
import io.axoniq.axonserver.grpc.internal.NodeReplicationGroup;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupEntry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupName;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RaftGroupService to use when the leader of the raft group is on a remote node. Sends all requests through GRPC calls.
 *
 * @author Marc Gathier
 */
public class RemoteRaftGroupService implements RaftGroupService {

    private static final Function<InstructionAck, Void> TO_VOID = x -> null;
    private static final Logger logger = LoggerFactory.getLogger(RemoteRaftGroupService.class);

    private final RaftGroupServiceGrpc.RaftGroupServiceStub stub;

    public RemoteRaftGroupService(RaftGroupServiceGrpc.RaftGroupServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public CompletableFuture<ReplicationGroupUpdateConfirmation> addServer(String replicationGroup, Node node) {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = new CompletableFuture<>();
        ReplicationGroupMember contextMember = ReplicationGroupMemberConverter.asContextMember(node);
        stub.addServer(ReplicationGroup.newBuilder().setName(replicationGroup).addMembers(contextMember).build(),
                       new CompletableStreamObserver<>(result, "addNodeToContext", logger));
        return result;
    }

    @Override
    public CompletableFuture<ReplicationGroupUpdateConfirmation> deleteServer(String context, String node) {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = new CompletableFuture<>();
        stub.removeServer(ReplicationGroup.newBuilder().setName(context)
                                          .addMembers(ReplicationGroupMember.newBuilder().setNodeId(node).build())
                                          .build(),
                          new CompletableStreamObserver<>(result, "deleteNode", logger));
        return result;
    }

    @Override
    public CompletableFuture<Void> appendEntry(String replicationGroup, String name, byte[] bytes) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ReplicationGroupEntry request = ReplicationGroupEntry.newBuilder()
                                                             .setReplicationGroupName(replicationGroup)
                                                             .setEntryName(name)
                                                             .setEntry(ByteString.copyFrom(bytes))
                                                             .build();
        stub.appendEntry(request,
                         new CompletableStreamObserver<>(result, "appendEntry", logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> getStatus(Consumer<ReplicationGroup> contextConsumer) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.getStatus(ReplicationGroup.getDefaultInstance(), new StreamObserver<ReplicationGroup>() {
            @Override
            public void onNext(ReplicationGroup context) {
                contextConsumer.accept(context);
            }

            @Override
            public void onError(Throwable throwable) {
                // log only
                logger.debug("Failed to retrieve status");
                result.complete(null);
            }

            @Override
            public void onCompleted() {
                // no further action needed
                result.complete(null);
            }
        });

        return result;
    }

    @Override
    public CompletableFuture<ReplicationGroupConfiguration> initReplicationGroup(String context,
                                                                                 List<Node> raftNodes) {
        CompletableFuture<ReplicationGroupConfiguration> result = new CompletableFuture<>();
        ReplicationGroup request = ReplicationGroup.newBuilder()
                                                   .setName(context)
                                                   .addAllMembers(raftNodes.stream()
                                                                           .map(ReplicationGroupMemberConverter::asContextMember)
                                                                           .collect(Collectors.toList()))
                                                   .build();
        Context.current().fork().wrap(() ->
                                              stub.initReplicationGroup(
                                                      request,
                                                      new CompletableStreamObserver<>(result, "initContext", logger)))
               .run();

        return result;
    }


    @Override
    public CompletableFuture<ReplicationGroupConfiguration> configuration(String replicationGroup) {
        CompletableFuture<ReplicationGroupConfiguration> result = new CompletableFuture<>();
        stub.configuration(ReplicationGroupName.newBuilder().setName(replicationGroup).build(),
                           new CompletableStreamObserver<>(result, "configuration", logger));
        return result;
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String replicationGroup) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.transferLeadership(ReplicationGroupName.newBuilder().setName(replicationGroup).build(),
                                new CompletableStreamObserver<>(result, "transferLeadership", logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> prepareDeleteNodeFromReplicationGroup(String replicationGroup, String node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.preDeleteNodeFromReplicationGroup(NodeReplicationGroup.newBuilder()
                                                                   .setNodeName(node)
                                                                   .setReplicationGroupName(replicationGroup)
                                                                   .build(),
                                               new CompletableStreamObserver<>(result,
                                                                               "prepareDeleteNodeFromContext",
                                                                               logger,
                                                                               TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteReplicationGroup(String replicationGroup, boolean preserveEventStore) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteReplicationGroup(DeleteReplicationGroupRequest.newBuilder()
                                                                 .setReplicationGroupName(replicationGroup)
                                                                 .setPreserveEventstore(preserveEventStore)
                                                                 .build(),
                                    new CompletableStreamObserver<>(result,
                                                                    "prepareDeleteNodeFromContext",
                                                                    logger,
                                                                    TO_VOID));
        return result;
    }
}
