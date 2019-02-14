package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextLoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Service
public class GrpcRaftGroupService extends RaftGroupServiceGrpc.RaftGroupServiceImplBase {
    private final LocalRaftGroupService localRaftGroupService;

    public GrpcRaftGroupService(LocalRaftGroupService localRaftGroupService) {
        this.localRaftGroupService = localRaftGroupService;
    }

    @Override
    public void initContext(Context request, StreamObserver<Confirmation> responseObserver) {
        try {
            localRaftGroupService.initContext(request.getName(), request.getMembersList()
                    .stream()
            .map(contextMember -> Node.newBuilder().setNodeId(contextMember.getNodeId()).setHost(contextMember.getHost()).setPort(contextMember.getPort()).build()).collect(
                            Collectors.toList()));
            responseObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (RuntimeException t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void addServer(Context request, StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.addNodeToContext(request.getName(), toNode(request.getMembers(0)));
        confirm(responseObserver, completable);
    }

    private void confirm(StreamObserver<Confirmation> responseObserver, CompletableFuture<Void> completable) {
        completable.whenComplete((r, t) -> {
            if (t != null) {
                responseObserver.onError(t);
            } else {
                responseObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void removeServer(Context request, StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.deleteNode(request.getName(), request.getMembers(0).getNodeId());
        confirm(responseObserver, completable);
    }


    @Override
    public void mergeAppAuthorization(ContextApplication request, StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateApplication(request);
        confirm(responseObserver, completable);
    }


    @Override
    public void mergeLoadBalanceStrategy(ContextLoadBalanceStrategy request,
                                         StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateLoadBalancingStrategy(request.getContext(), request.getLoadBalanceStrategy());
        confirm(responseObserver, completable);
    }

    @Override
    public void deleteLoadBalanceStrategy(ContextLoadBalanceStrategy request,
                                          StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.deleteLoadBalancingStrategy(request.getContext(), request.getLoadBalanceStrategy());
        confirm(responseObserver, completable);
    }

    @Override
    public void mergeProcessorLBStrategy(ContextProcessorLBStrategy request,
                                         StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Void> completable = localRaftGroupService.updateProcessorLoadBalancing(request.getContext(), request.getProcessorLBStrategy());
        confirm(responseObserver, completable);
    }

    @Override
    public void getStatus(Context request, StreamObserver<Context> responseObserver) {
        localRaftGroupService.getStatus(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    private Node toNode(ContextMember member) {
        return Node.newBuilder().setPort(member.getPort()).setHost(member.getHost()).setNodeId(member.getNodeId()).build();
    }

}
