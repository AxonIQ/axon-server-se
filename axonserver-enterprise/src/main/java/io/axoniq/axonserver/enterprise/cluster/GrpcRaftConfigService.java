package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextNames;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.User;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

/**
 * Author: marc
 */
@Service
public class GrpcRaftConfigService extends RaftConfigServiceGrpc.RaftConfigServiceImplBase {
    private final GrpcRaftController raftController;

    public GrpcRaftConfigService(GrpcRaftController raftController) {
        this.raftController = raftController;
    }

    @Override
    public void initCluster(ContextNames request, StreamObserver<Confirmation> responseObserver) {
        super.initCluster(request, responseObserver);
    }

    @Override
    public void joinCluster(NodeInfo request, StreamObserver<Confirmation> responseObserver) {
        raftController.join(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void createContext(Context request, StreamObserver<Confirmation> responseObserver) {
        super.createContext(request, responseObserver);
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
    public void addApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        super.addApplication(request, responseObserver);
    }

    @Override
    public void updateApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        super.updateApplication(request, responseObserver);
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        super.deleteApplication(request, responseObserver);
    }

    @Override
    public void addUser(User request, StreamObserver<Confirmation> responseObserver) {
        super.addUser(request, responseObserver);
    }

    @Override
    public void updateUser(User request, StreamObserver<Confirmation> responseObserver) {
        super.updateUser(request, responseObserver);
    }

    @Override
    public void deleteUser(User request, StreamObserver<Confirmation> responseObserver) {
        super.deleteUser(request, responseObserver);
    }
}
