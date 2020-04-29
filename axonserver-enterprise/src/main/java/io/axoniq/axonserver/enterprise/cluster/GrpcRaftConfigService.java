package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.internal.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

/**
 * Binding of {@link RaftConfigService} gRPC API.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Service
public class GrpcRaftConfigService extends RaftConfigServiceGrpc.RaftConfigServiceImplBase implements
        AxonServerInternalService {

    private final Logger logger = LoggerFactory.getLogger(GrpcRaftConfigService.class);
    private final LocalRaftConfigService localRaftConfigService;
    private final Supplier<RaftConfigService> serviceFactory;

    @Autowired
    public GrpcRaftConfigService(LocalRaftConfigService localRaftConfigService,
                                 RaftConfigServiceFactory raftConfigServiceFactory) {
        this(localRaftConfigService, raftConfigServiceFactory::getRaftConfigService);
    }

    /**
     * Creates an GrpcRaftConfigService instance with specified {@link LocalRaftConfigService} and supplier of
     * admin leader's {@link RaftConfigService}
     *
     * @param localRaftConfigService the local RaftConfigService instance
     * @param serviceFactory         the supplier of the RaftConfigService instance for current leader of _admin group
     */
    GrpcRaftConfigService(LocalRaftConfigService localRaftConfigService, Supplier<RaftConfigService> serviceFactory) {
        this.localRaftConfigService = localRaftConfigService;
        this.serviceFactory = serviceFactory;
    }

    @Override
    public void initCluster(ContextNames request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.init(request.getContextsList()));
    }

    @Override
    public void joinCluster(NodeInfo request, StreamObserver<UpdateLicense> responseObserver) {
        logger.info("{}:{} wants to join cluster with name {}",
                request.getInternalHostName(),
                request.getGrpcInternalPort(),
                request.getNodeName());

        try {
            responseObserver.onNext(serviceFactory.get().join(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(GrpcExceptionBuilder.build(e));
        }
    }

    @Override
    public void createContext(Context request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.addContext(
                request));
    }

    @Override
    public void deleteNode(NodeName request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteNode(request.getNode()));
    }

    private void wrap(StreamObserver<InstructionAck> responseObserver, Runnable action) {
        try {
            io.grpc.Context.current().fork().wrap(action).run();
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void addNodeToContext(NodeContext request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver,
                () -> localRaftConfigService
                        .addNodeToContext(request.getContext(), request.getNodeName(), request.getRole()));
    }

    @Override
    public void deleteNodeFromContext(NodeContext request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver,
                () -> localRaftConfigService.deleteNodeFromContext(request.getContext(), request.getNodeName()));
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
    public void updateUser(User request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.updateUser(request));
    }

    @Override
    public void deleteUser(User request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteUser(request));
    }

    @Override
    public void deleteContext(ContextName request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteContext(request.getContext()));
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteApplication(request));
    }

    @Override
    public void updateLoadBalanceStrategy(LoadBalanceStrategy request,
                                          StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.updateLoadBalancingStrategy(request));
    }

    @Override
    public void deleteLoadBalanceStrategy(LoadBalanceStrategy request,
                                          StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteLoadBalancingStrategy(request));
    }

    @Override
    public void updateProcessorLBStrategy(ProcessorLBStrategy request,
                                          StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.updateProcessorLoadBalancing(request));
    }
}
