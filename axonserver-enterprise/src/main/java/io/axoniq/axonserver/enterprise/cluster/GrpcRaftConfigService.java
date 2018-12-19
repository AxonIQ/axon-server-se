package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextNames;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.User;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Service
public class GrpcRaftConfigService extends RaftConfigServiceGrpc.RaftConfigServiceImplBase {

    private final LocalRaftConfigService localRaftConfigService;

    public GrpcRaftConfigService(LocalRaftConfigService localRaftConfigService) {
        this.localRaftConfigService = localRaftConfigService;
    }

    @Override
    public void initCluster(ContextNames request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.init(request.getContextsList());
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void joinCluster(NodeInfo request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.join(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void createContext(Context request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.addContext(request.getName(),
                                                           request.getMembersList()
                                                                  .stream()
                                                                  .map(ContextMember::getNodeId)
                                                                  .collect(Collectors.toList()));
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void addNodeToContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.addNodeToContext(request.getContext(), request.getNode());
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteNodeFromContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.deleteNodeFromContext(request.getContext(), request.getNode());
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.updateApplication( request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateUser(User request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.updateUser( request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteUser(User request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.deleteUser(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteContext(ContextName request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.deleteContext(request.getContext());
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.deleteApplication(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateLoadBalanceStrategy(LoadBalanceStrategy request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.updateLoadBalancingStrategy(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteLoadBalanceStrategy(LoadBalanceStrategy request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.deleteLoadBalancingStrategy(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProcessorLBStrategy(ProcessorLBStrategy request, StreamObserver<Confirmation> responseObserver) {
        localRaftConfigService.updateProcessorLoadBalancing(request);
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

}
