package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextNames;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeName;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.User;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Service
public class GrpcRaftConfigService extends RaftConfigServiceGrpc.RaftConfigServiceImplBase {
    private static final Confirmation CONFIRMATION = Confirmation.newBuilder().setSuccess(true).build();

    private final LocalRaftConfigService localRaftConfigService;
    private final Supplier<RaftConfigService> serviceFactory;

    @Autowired
    public GrpcRaftConfigService(LocalRaftConfigService localRaftConfigService,
                                 RaftConfigServiceFactory raftConfigServiceFactory) {
        this(localRaftConfigService, raftConfigServiceFactory::getRaftConfigService);
    }

    GrpcRaftConfigService(LocalRaftConfigService localRaftConfigService, Supplier<RaftConfigService> serviceFactory){
        this.localRaftConfigService = localRaftConfigService;
        this.serviceFactory = serviceFactory;
    }

    @Override
    public void initCluster(ContextNames request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.init(request.getContextsList()));
    }

    @Override
    public void joinCluster(NodeInfo request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> serviceFactory.get().join(request));
    }

    @Override
    public void createContext(Context request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.addContext(request.getName(),
                                                                      request.getMembersList()
                                                                             .stream()
                                                                             .map(ContextMember::getNodeId)
                                                                             .collect(Collectors.toList())));
    }

    @Override
    public void deleteNode(NodeName request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteNode(request.getNode()));
    }

    private void wrap(StreamObserver<Confirmation> responseObserver, Runnable action) {
        try {
            io.grpc.Context.current().fork().wrap(action).run();
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void addNodeToContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()->localRaftConfigService.addNodeToContext(request.getContext(), request.getNodeName()));
    }

    @Override
    public void deleteNodeFromContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()->localRaftConfigService.deleteNodeFromContext(request.getContext(), request.getNodeName()));
    }

    @Override
    public void updateApplication(Application request, StreamObserver<Application> responseObserver) {
            try {
                responseObserver.onNext(localRaftConfigService.updateApplication(request));
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
    }

    @Override
    public void refreshToken(Application request, StreamObserver<Application> responseObserver) {
        try {
            responseObserver.onNext(localRaftConfigService.refreshToken(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(GrpcExceptionBuilder.build(e));
        }
    }

    @Override
    public void updateUser(User request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.updateUser( request));
    }

    @Override
    public void deleteUser(User request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.deleteUser(request));
    }

    @Override
    public void deleteContext(ContextName request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.deleteContext(request.getContext()));
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.deleteApplication(request));
    }

    @Override
    public void updateLoadBalanceStrategy(LoadBalanceStrategy request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.updateLoadBalancingStrategy(request));
    }

    @Override
    public void deleteLoadBalanceStrategy(LoadBalanceStrategy request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.deleteLoadBalancingStrategy(request));
    }

    @Override
    public void updateProcessorLBStrategy(ProcessorLBStrategy request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver, ()-> localRaftConfigService.updateProcessorLoadBalancing(request));
    }

}
