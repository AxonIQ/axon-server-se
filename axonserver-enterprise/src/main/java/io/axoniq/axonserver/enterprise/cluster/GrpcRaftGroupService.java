package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Service
public class GrpcRaftGroupService extends RaftGroupServiceGrpc.RaftGroupServiceImplBase {
    private final GrpcRaftController controller;

    public GrpcRaftGroupService(GrpcRaftController controller) {
        this.controller = controller;
    }

    @Override
    public void initContext(Context request, StreamObserver<Confirmation> responseObserver) {
        try {
            controller.localRaftGroupService().initContext(request.getName(), request.getMembersList()
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
        CompletableFuture<Void> completable = controller.localRaftGroupService().addNodeToContext(request.getName(), toNode(request.getMembers(0)));
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
        CompletableFuture<Void> completable = controller.localRaftGroupService().deleteNode(request.getName(), request.getMembers(0).getNodeId());
        confirm(responseObserver, completable);
    }

    private Node toNode(ContextMember member) {
        return Node.newBuilder().setPort(member.getPort()).setHost(member.getHost()).setNodeId(member.getNodeId()).build();
    }

    @Override
    public void getStatus(Context request, StreamObserver<Context> responseObserver) {
        controller.localRaftGroupService().getStatus(responseObserver::onNext);
        responseObserver.onCompleted();
    }
}
