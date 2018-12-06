package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

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
            controller.initRaftGroup(request.getName());
            responseObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void addNodeToContext(Context request, StreamObserver<Confirmation> responseObserver) {
        super.addNodeToContext(request, responseObserver);
    }

    @Override
    public void deleteNodeFromContext(Context request, StreamObserver<Confirmation> responseObserver) {
        super.deleteNodeFromContext(request, responseObserver);
    }

    @Override
    public void updateApplicationInContext(Application request, StreamObserver<Confirmation> responseObserver) {
        super.updateApplicationInContext(request, responseObserver);
    }

    @Override
    public void deleteApplicationFromContext(Application request, StreamObserver<Confirmation> responseObserver) {
        super.deleteApplicationFromContext(request, responseObserver);
    }

    @Override
    public void getStatus(Context request, StreamObserver<Context> responseObserver) {
        super.getStatus(request, responseObserver);
    }
}
