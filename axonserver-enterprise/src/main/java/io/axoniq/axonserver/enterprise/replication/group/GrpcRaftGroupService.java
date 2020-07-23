package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.ReplicationGroupMemberConverter;
import io.axoniq.axonserver.grpc.internal.DeleteReplicationGroupRequest;
import io.axoniq.axonserver.grpc.internal.NodeReplicationGroup;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupEntry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupName;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * GRPC endpoint for the RaftGroupServiceGrpc service. Implements the handlers for RPCs in this service.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Service
public class GrpcRaftGroupService extends RaftGroupServiceGrpc.RaftGroupServiceImplBase implements
        AxonServerInternalService {

    private final Logger logger = LoggerFactory.getLogger(GrpcRaftGroupService.class);
    private final LocalRaftGroupService localRaftGroupService;

    /**
     * Constructor of the service
     *
     * @param localRaftGroupService delegate to perform the actual work
     */
    public GrpcRaftGroupService(LocalRaftGroupService localRaftGroupService) {
        this.localRaftGroupService = localRaftGroupService;
    }

    /**
     * Initializes a context on this node.
     *
     * @param request          the context to create
     * @param responseObserver observer for the results
     */
    @Override
    public void initReplicationGroup(ReplicationGroup request,
                                     StreamObserver<ReplicationGroupConfiguration> responseObserver) {
        logger.debug("Init replication group: {}", request);
        try {
            ReplicationGroupConfiguration contextConfiguration =
                    localRaftGroupService.initReplicationGroup(request.getName(),
                                                               request.getMembersList()
                                                                      .stream()
                                                                      .map(ReplicationGroupMemberConverter::asNode)
                                                                      .collect(Collectors
                                                                                       .toList()))
                                         .get();

            responseObserver.onNext(contextConfiguration);
            responseObserver.onCompleted();
        } catch (Exception t) {
            logger.warn("Init replication group: {}", request, t);
            responseObserver.onError(GrpcExceptionBuilder.build(t));
        }
    }

    @Override
    public void addServer(ReplicationGroup request,
                          StreamObserver<ReplicationGroupUpdateConfirmation> responseObserver) {
        try {
            CompletableFuture<ReplicationGroupUpdateConfirmation> completable = localRaftGroupService
                    .addServer(request.getName(), ReplicationGroupMemberConverter
                            .asNode(request.getMembers(0)));
            forwardWhenComplete(responseObserver, completable);
        } catch (Exception t) {
            logger.warn("Add server: {}", request, t);
            responseObserver.onError(GrpcExceptionBuilder.build(t));
        }
    }

    private <T> void forwardWhenComplete(StreamObserver<T> responseObserver, CompletableFuture<T> completable) {
        completable.whenComplete((r, t) -> {
            if (t != null) {
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            } else {
                responseObserver.onNext(r);
                responseObserver.onCompleted();
            }
        });
    }

    private void confirm(StreamObserver<InstructionAck> responseObserver, CompletableFuture<Void> completable) {
        completable.whenComplete((r, t) -> {
            if (t != null) {
                responseObserver.onError(GrpcExceptionBuilder.build(t));
            } else {
                responseObserver.onNext(InstructionAck.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void removeServer(ReplicationGroup request,
                             StreamObserver<ReplicationGroupUpdateConfirmation> responseObserver) {
        try {
            CompletableFuture<ReplicationGroupUpdateConfirmation> completable = localRaftGroupService
                    .deleteServer(request.getName(),
                                  request.getMembers(0)
                                         .getNodeId());
            forwardWhenComplete(responseObserver, completable);
        } catch (Exception ex) {
            logger.warn("remove server {}", request, ex);
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void deleteReplicationGroup(DeleteReplicationGroupRequest request,
                                       StreamObserver<InstructionAck> responseObserver) {
        try {
            CompletableFuture<Void> completable = localRaftGroupService
                    .deleteReplicationGroup(request.getReplicationGroupName(),
                                            request.getPreserveEventstore());
            confirm(responseObserver, completable);
        } catch (Exception ex) {
            logger.warn("Failed to delete replication group: {}", request.getReplicationGroupName(), ex);
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void appendEntry(ReplicationGroupEntry request, StreamObserver<InstructionAck> responseObserver) {
        try {
            CompletableFuture<Void> completable = localRaftGroupService.appendEntry(request.getReplicationGroupName(),
                                                                                    request.getEntryName(),
                                                                                    request.getEntry().toByteArray());
            confirm(responseObserver, completable);
        } catch (Exception ex) {
            logger.warn("Append Entry for: {}", request.getReplicationGroupName(), ex);
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void getStatus(ReplicationGroup request, StreamObserver<ReplicationGroup> responseObserver) {
        try {
            localRaftGroupService.getStatus(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (Exception t) {
            logger.warn("Get status: {}", request, t);
            responseObserver.onError(GrpcExceptionBuilder.build(t));
        }
    }

    @Override
    public void configuration(ReplicationGroupName request,
                              StreamObserver<ReplicationGroupConfiguration> responseObserver) {
        try {
            localRaftGroupService.configuration(request.getName())
                                 .thenAccept(c -> {
                                     responseObserver.onNext(c);
                                     responseObserver.onCompleted();
                                 }).exceptionally(cause -> {
                responseObserver.onError(GrpcExceptionBuilder.build(cause));
                return null;
            });
        } catch (Exception t) {
            logger.warn("Configuration: {}", request, t);
            responseObserver.onError(GrpcExceptionBuilder.build(t));
        }
    }

    @Override
    public void transferLeadership(ReplicationGroupName request, StreamObserver<InstructionAck> responseObserver) {
        io.grpc.Context.current().fork().wrap(() -> doTransferLeadership(request, responseObserver)).run();
    }

    private void doTransferLeadership(ReplicationGroupName request,
                                      StreamObserver<InstructionAck> responseObserver) {
        try {
            localRaftGroupService.transferLeadership(request.getName())
                                 .thenAccept(c -> {
                                     responseObserver.onNext(InstructionAck.newBuilder().setSuccess(true).build());
                                     responseObserver.onCompleted();
                                 }).exceptionally(cause -> {
                responseObserver.onError(GrpcExceptionBuilder.build(cause));
                return null;
            });
        } catch (Exception t) {
            logger.warn("transfer leadership: {}", request, t);
            responseObserver.onError(GrpcExceptionBuilder.build(t));
        }
    }

    @Override
    public void preDeleteNodeFromReplicationGroup(NodeReplicationGroup request,
                                                  StreamObserver<InstructionAck> responseObserver) {
        try {
            localRaftGroupService.prepareDeleteNodeFromReplicationGroup(request.getReplicationGroupName(),
                                                                        request.getNodeName())
                                 .thenAccept(c -> {
                                     responseObserver.onNext(InstructionAck.newBuilder().setSuccess(true).build());
                                     responseObserver.onCompleted();
                                 }).exceptionally(cause -> {
                responseObserver.onError(GrpcExceptionBuilder.build(cause));
                return null;
            });
        } catch (Exception t) {
            logger.warn("pre-delete node from replication group: {}", request, t);
            responseObserver.onError(GrpcExceptionBuilder.build(t));
        }
    }
}
