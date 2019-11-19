package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.ContextMemberConverter;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextEntry;
import io.axoniq.axonserver.grpc.internal.ContextLoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Service
public class GrpcRaftGroupService extends RaftGroupServiceGrpc.RaftGroupServiceImplBase {

    private final Logger logger = LoggerFactory.getLogger(GrpcRaftGroupService.class);
    private final LocalRaftGroupService localRaftGroupService;

    public GrpcRaftGroupService(LocalRaftGroupService localRaftGroupService) {
        this.localRaftGroupService = localRaftGroupService;
    }

    @Override
    public void initContext(Context request, StreamObserver<ContextConfiguration> responseObserver) {
        logger.debug("Init context: {}", request);
        try {
            ContextConfiguration contextConfiguration = localRaftGroupService.initContext(request.getName(),
                                                                                          request.getMembersList()
                                                                                                 .stream()
                                                                                                 .map(contextMember -> Node
                                                                                                         .newBuilder()
                                                                                                         .setNodeId(
                                                                                                                 contextMember
                                                                                                                         .getNodeId())
                                                                                                         .setHost(
                                                                                                                 contextMember
                                                                                                                         .getHost())
                                                                                                         .setPort(
                                                                                                                 contextMember
                                                                                                                         .getPort())
                                                                                                         .setNodeName(
                                                                                                                 contextMember
                                                                                                                         .getNodeName())
                                                                                                         .build())
                                                                                                 .collect(Collectors
                                                                                                                  .toList()))
                                                                             .get();

            responseObserver.onNext(contextConfiguration);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            logger.warn("Init context failed: {}", request, t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void addServer(Context request, StreamObserver<ContextUpdateConfirmation> responseObserver) {
        CompletableFuture<ContextUpdateConfirmation> completable = localRaftGroupService
                .addNodeToContext(request.getName(), ContextMemberConverter.asNode(request.getMembers(0)));
        forwardWhenComplete(responseObserver, completable);
    }

    private <T> void forwardWhenComplete(StreamObserver<T> responseObserver, CompletableFuture<T> completable) {
        completable.whenComplete((r, t) -> {
            if (t != null) {
                responseObserver.onError(t);
            } else {
                responseObserver.onNext(r);
                responseObserver.onCompleted();
            }
        });
    }

    private void confirm(StreamObserver<InstructionAck> responseObserver, CompletableFuture<Void> completable) {
        completable.whenComplete((r, t) -> {
            if (t != null) {
                responseObserver.onError(t);
            } else {
                responseObserver.onNext(InstructionAck.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void removeServer(Context request, StreamObserver<ContextUpdateConfirmation> responseObserver) {
        CompletableFuture<ContextUpdateConfirmation> completable = localRaftGroupService.deleteNode(request.getName(),
                                                                                                    request.getMembers(0)
                                                                                                           .getNodeId());
        forwardWhenComplete(responseObserver, completable);
    }

    @Override
    public void deleteContext(DeleteContextRequest request, StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.deleteContext(request.getContext(),
                                                                                  request.getPreserveEventstore());
        confirm(responseObserver, completable);
    }

    @Override
    public void appendEntry(ContextEntry request, StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.appendEntry(request.getContext(),
                                                                                request.getEntryName(),
                                                                                request.getEntry().toByteArray());
        confirm(responseObserver, completable);
    }

    @Override
    public void mergeAppAuthorization(ContextApplication request, StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateApplication(request);
        confirm(responseObserver, completable);
    }

    @Override
    public void deleteAppAuthorization(ContextApplication request, StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.deleteApplication(request);
        confirm(responseObserver, completable);
    }

    @Override
    public void mergeUserAuthorization(ContextUser request, StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateUser(request);
        confirm(responseObserver, completable);
    }

    @Override
    public void deleteUserAuthorization(ContextUser request, StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.deleteUser(request);
        confirm(responseObserver, completable);
    }

    @Override
    public void mergeLoadBalanceStrategy(ContextLoadBalanceStrategy request,
                                         StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateLoadBalancingStrategy(request.getContext(),
                                                                                                request.getLoadBalanceStrategy());
        confirm(responseObserver, completable);
    }

    @Override
    public void deleteLoadBalanceStrategy(ContextLoadBalanceStrategy request,
                                          StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.deleteLoadBalancingStrategy(request.getContext(),
                                                                                                request.getLoadBalanceStrategy());
        confirm(responseObserver, completable);
    }

    @Override
    public void mergeProcessorLBStrategy(ContextProcessorLBStrategy request,
                                         StreamObserver<InstructionAck> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateProcessorLoadBalancing(request.getContext(),
                                                                                                 request.getProcessorLBStrategy());
        confirm(responseObserver, completable);
    }

    @Override
    public void getStatus(Context request, StreamObserver<Context> responseObserver) {
        localRaftGroupService.getStatus(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void configuration(ContextName request, StreamObserver<ContextConfiguration> responseObserver) {
        localRaftGroupService.configuration(request.getContext())
                             .thenAccept(c -> {
                                 responseObserver.onNext(c);
                                 responseObserver.onCompleted();
                             });
    }

    @Override
    public void transferLeadership(ContextName request, StreamObserver<InstructionAck> responseObserver) {
        io.grpc.Context.current().fork().wrap(() -> doTransferLeadership(request, responseObserver)).run();
    }

    @Override
    public void preDeleteNodeFromContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.prepareDeleteNodeFromContext(request.getContext(),
                                                                                                 request.getNodeName());
        confirm(responseObserver, completable);
    }

    private void doTransferLeadership(ContextName request,
                                      StreamObserver<InstructionAck> responseObserver) {
        localRaftGroupService.transferLeadership(request.getContext())
                             .thenAccept(c -> {
                                 responseObserver.onNext(InstructionAck.newBuilder().setSuccess(true).build());
                                 responseObserver.onCompleted();
                             }).exceptionally(cause -> {
            responseObserver.onError(cause);
            return null;
        });
    }
}
