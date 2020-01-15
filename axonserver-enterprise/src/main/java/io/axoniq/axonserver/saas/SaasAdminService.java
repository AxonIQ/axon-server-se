package io.axoniq.axonserver.saas;

import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.access.application.JpaApplicationRepository;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigService;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.EmptyRequest;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.saas.SaasAdminServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides an interface to be used by Axon Cloud Console to update configuration in Axon Server.
 * Only enabled when profile axoniq-cloud-support is enabled
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Service
@Profile("axoniq-cloud-support")
public class SaasAdminService extends SaasAdminServiceGrpc.SaasAdminServiceImplBase implements
        AxonServerInternalService {

    private static final InstructionAck CONFIRMATION = InstructionAck.newBuilder().setSuccess(true).build();
    private final Supplier<Stream<io.axoniq.axonserver.enterprise.jpa.ClusterNode>> clusterProvider;
    private final Supplier<Stream<io.axoniq.axonserver.enterprise.jpa.Context>> contextProvider;
    private final Supplier<List<JpaApplication>> applicationProvider;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final Supplier<RaftConfigService> raftConfigServiceProvider;

    @Autowired
    public SaasAdminService(RaftConfigServiceFactory raftConfigServiceFactory,
                            ClusterController clusterController,
                            ContextController contextController,
                            JpaApplicationRepository applicationRepository) {
        this(clusterController::activeNodes,
             contextController::getContexts,
             applicationRepository::findAll,
             raftConfigServiceFactory::getRaftConfigService);
    }

    public SaasAdminService(
            Supplier<Stream<ClusterNode>> clusterProvider,
            Supplier<Stream<io.axoniq.axonserver.enterprise.jpa.Context>> contextProvider,
            Supplier<List<JpaApplication>> applicationProvider,
            Supplier<RaftConfigService> raftConfigServiceProvider) {
        this.clusterProvider = clusterProvider;
        this.contextProvider = contextProvider;
        this.applicationProvider = applicationProvider;
        this.raftConfigServiceProvider = raftConfigServiceProvider;
    }

    /**
     * Retrieves all nodes in the cluster and their contexts.
     *
     * @param request          empty request, gRPC requires a request message
     * @param responseObserver observer where nodes are published to. Each node is published as a separate response.
     */
    @Override
    public void getNodes(EmptyRequest request, StreamObserver<NodeInfo> responseObserver) {
        clusterProvider.get().forEach(n -> responseObserver.onNext(toNodeInfo(n)));
        responseObserver.onCompleted();
    }

    private NodeInfo toNodeInfo(ClusterNode n) {
        return NodeInfo.newBuilder(n.toNodeInfo())
                       .addAllContexts(n.getContexts()
                                        .stream()
                                        .map(ccn -> ContextRole.newBuilder()
                                                               .setName(ccn.getContext().getName())
                                                               .build())
                                        .collect(Collectors.toList()))
                       .build();
    }

    /**
     * Retrieves all contexts, and the nodes they are assigned to.
     * @param request empty request, gRPC requires a request message
     * @param responseObserver observer where contexts are published to. Each context is published as a separate response.
     */
    @Override
    public void getContexts(EmptyRequest request, StreamObserver<Context> responseObserver) {
        contextProvider.get().forEach(c -> {
            responseObserver.onNext(Context.newBuilder().setName(c.getName())
                                           .addAllMembers(c.getNodes().stream().map(ccn -> ContextMember.newBuilder()
                                                                                                        .setNodeName(
                                                                                                                ccn.getClusterNode()
                                                                                                                   .getName())
                                                                                                        .build())
                                                           .collect(
                                                                   Collectors.toList()))
                                           .putAllMetaData(c.getMetaDataMap()).build());
        });

        responseObserver.onCompleted();
    }

    /**
     * Retrieves all applications, and the assigned roles.
     * @param request empty request, gRPC requires a request message
     * @param responseObserver observer where applications are published to. Each application is published as a separate response.
     */
    @Override
    public void getApplications(EmptyRequest request, StreamObserver<Application> responseObserver) {
        applicationProvider.get().forEach(app -> responseObserver
                .onNext(ApplicationProtoConverter.createApplication(app)));
        responseObserver.onCompleted();
    }

    /**
     * Creates a new context. If there is a meta data value {@code nodes} defined in the request, it will add the context to
     * that number of nodes. If this is not specified it will add it to one node.
     * The operation assigns the context to the nodes with the lowest number of contexts available.
     *
     * @param request the context to create
     * @param responseObserver acknowledgement on successful completion
     */
    @Override
    public void createContext(Context request, StreamObserver<InstructionAck> responseObserver) {
        try {

            if (!contextNameValidation.test(request.getName())) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME, "Invalid context name");
            }

            int nodes = Integer.valueOf(request.getMetaDataMap().getOrDefault("nodes", "1"));
            if (nodes < 1) {
                throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED,
                                                     "Invalid number of nodes: " + nodes);
            }


            List<ContextMember> selectedNodes = clusterProvider.get().sorted(Comparator.comparingInt(c -> c
                    .getContexts().size()))
                                                                 .limit(nodes)
                                                                 .map(c -> ContextMember.newBuilder()
                                                                                        .setNodeId(c.getName())
                                                                                        .setNodeName(c.getName())
                                                                                        .setHost(c.getInternalHostName())
                                                                                        .setPort(c.getGrpcInternalPort())
                                                                                        .build()
                                                                 )
                                                                 .collect(Collectors.toList());
            if (selectedNodes.size() < nodes) {
                throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED,
                                                     "Invalid number of nodes: " + nodes);
            }

            Context updatedContext = Context.newBuilder(request)
                                            .clearMembers()
                                            .addAllMembers(selectedNodes)
                                            .build();
            raftConfigServiceProvider.get().addContext(updatedContext);
            responseObserver.onNext(CONFIRMATION);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void addNodeToContext(NodeContext request, StreamObserver<InstructionAck> responseObserver) {
        try {
            raftConfigServiceProvider.get().addNodeToContext(request.getContext(),
                                                             request.getNodeName(),
                                                             Role.PRIMARY);
            responseObserver.onNext(CONFIRMATION);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void deleteNodeFromContext(NodeContext request, StreamObserver<InstructionAck> responseObserver) {
        try {
            raftConfigServiceProvider.get().deleteNodeFromContext(request.getContext(),
                                                                  request.getNodeName());
            responseObserver.onNext(CONFIRMATION);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void deleteContext(ContextName request, StreamObserver<InstructionAck> responseObserver) {
        wrap(responseObserver,
             () -> raftConfigServiceProvider.get().deleteContext(request.getContext()));
    }

    /**
     * Create a new application and grants roles to given contexts.
     * @param request the application and grants
     * @param responseObserver stream where application is returned when created
     */
    @Override
    public void createApplication(Application request, StreamObserver<Application> responseObserver) {

        try {
            Application response = raftConfigServiceProvider.get().updateApplication(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<InstructionAck> responseObserver) {
        try {
            raftConfigServiceProvider.get().deleteApplication(request);
            responseObserver.onNext(CONFIRMATION);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    /**
     * Refreshes the token for a specific application.
     * @param request contains the application to refresh the token
     * @param responseObserver updated application, with new token inside
     */
    @Override
    public void refreshToken(Application request, StreamObserver<Application> responseObserver) {
        try {
            Application response = raftConfigServiceProvider.get().refreshToken(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    private void wrap(StreamObserver<InstructionAck> responseObserver, Runnable action) {
        try {
            io.grpc.Context.current().fork().wrap(action).run();
            responseObserver.onNext(CONFIRMATION);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }
}
