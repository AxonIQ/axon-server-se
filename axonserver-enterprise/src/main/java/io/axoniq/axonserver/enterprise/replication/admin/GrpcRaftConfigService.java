package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ContextNames;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.DeleteReplicationGroupRequest;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeName;
import io.axoniq.axonserver.grpc.internal.NodeReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContext;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.axoniq.axonserver.grpc.internal.User;
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

    /**
     * Initializes this node as the first node of a cluster. Creates a replication group and context for each context
     * name specified in the request.
     *
     * @param request          the contexts to be created
     * @param responseObserver response stream observer
     */
    @Override
    public void initCluster(ContextNames request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.init(request.getContextsList()));
    }

    /**
     * Handles request from another node that wants to join this cluster.
     *
     * @param request          the information on the node that wants to join the cluster
     * @param responseObserver response stream observer
     */
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

    /**
     * Handles request to create a new context.
     *
     * @param request          the request containing the name of the replication group and information on the context
     *                         to create
     * @param responseObserver response stream observer
     */
    @Override
    public void createContext(ReplicationGroupContext request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.addContext(
                request.getReplicationGroupName(), request.getContextName(), request.getMetaDataMap()));
    }

    /**
     * Handles the request to delete a node from the cluster.
     *
     * @param request          contains the node name
     * @param responseObserver response stream observer
     */
    @Override
    public void deleteNode(NodeName request, StreamObserver<InstructionAck> responseObserver) {
        if (request.getRequireEmpty()) {
            wrap(responseObserver, () -> localRaftConfigService.deleteNodeIfEmpty(request.getNode()));
        } else {
            wrap(responseObserver, () -> localRaftConfigService.deleteNode(request.getNode()));
        }
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

    /**
     * Handles the request to add a node to an already existing replication group.
     *
     * @param request          contains node and replication group name
     * @param responseObserver response stream observer
     */
    @Override
    public void addNodeToReplicationGroup(NodeReplicationGroup request,
                                          StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver,
                () -> localRaftConfigService
                        .addNodeToReplicationGroup(request.getReplicationGroupName(),
                                                request.getNodeName(),
                                                request.getRole()));
    }

    /**
     * Handles the request to delete a node from an already existing replication group. Request contains flag to
     * specify that the event store contents must be preserved for the contexts in the replication group.
     *
     * @param request          contains node and replication group name
     * @param responseObserver response stream observer
     */
    @Override
    public void deleteNodeFromReplicationGroup(NodeReplicationGroup request,
                                               StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver,
                () -> localRaftConfigService
                     .deleteNodeFromReplicationGroup(request.getReplicationGroupName(), request.getNodeName()));
    }

    /**
     * Handles request to update the application binding information.
     *
     * @param request          the application binding information
     * @param responseObserver response stream observer
     */
    @Override
    public void updateApplication(Application request, StreamObserver<Application> responseObserver) {
        try {
            responseObserver.onNext(localRaftConfigService.updateApplication(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    /**
     * Handles request to update the access token for an existing application binding.
     *
     * @param request          the application binding information
     * @param responseObserver response stream observer
     */
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
    public void deleteContext(DeleteContextRequest request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver,
             () -> localRaftConfigService.deleteContext(request.getContext(), request.getPreserveEventstore()));
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteApplication(request));
    }

    @Override
    public void updateProcessorLBStrategy(ProcessorLBStrategy request,
                                          StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.updateProcessorLoadBalancing(request));
    }

    @Override
    public void createReplicationGroup(ReplicationGroup request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.createReplicationGroup(request.getName(),
                                                                                   request.getMembersList()
        ));
    }

    @Override
    public void deleteReplicationGroup(DeleteReplicationGroupRequest request,
                                       StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver, () -> localRaftConfigService.deleteReplicationGroup(request.getReplicationGroupName(),
                                                                                   request.getPreserveEventstore()));
    }
}
