package io.axoniq.axonserver.saas;

import io.axoniq.axonserver.access.application.JpaApplicationRepository;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
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
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Service
public class SaasAdminService extends SaasAdminServiceGrpc.SaasAdminServiceImplBase {

    private final RaftConfigServiceFactory raftConfigServiceFactory;
    private final ClusterController clusterController;
    private final ContextController contextController;
    private final JpaApplicationRepository applicationRepository;

    public SaasAdminService(RaftConfigServiceFactory raftConfigServiceFactory,
                            ClusterController clusterController,
                            ContextController contextController,
                            JpaApplicationRepository applicationRepository) {
        this.raftConfigServiceFactory = raftConfigServiceFactory;
        this.clusterController = clusterController;
        this.contextController = contextController;
        this.applicationRepository = applicationRepository;
    }

    @Override
    public void getNodes(EmptyRequest request, StreamObserver<NodeInfo> responseObserver) {
        clusterController.nodes().forEach(n -> responseObserver.onNext(toNodeInfo(n)));
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

    @Override
    public void getContexts(EmptyRequest request, StreamObserver<Context> responseObserver) {
        contextController.getContexts().forEach(c -> {
            responseObserver.onNext(Context.newBuilder().setName(c.getName())
                                           .addAllMembers(c.getAllNodes().stream().map(ccn -> ContextMember.newBuilder()
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

    @Override
    public void getApplications(EmptyRequest request, StreamObserver<Application> responseObserver) {
        applicationRepository.findAll().forEach(app -> responseObserver
                .onNext(ApplicationProtoConverter.createApplication(app)));
        responseObserver.onCompleted();
    }

    @Override
    public void createContext(Context request, StreamObserver<Confirmation> responseObserver) {
        try {
            int nodes = Integer.valueOf(request.getMetaDataMap().getOrDefault("nodes", "3"));

            List<ContextMember> selectedNodes = clusterController.nodes().sorted(Comparator.comparingInt(c -> c
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
                throw new RuntimeException("Not enough nodes available");
            }

            Context updatedContext = Context.newBuilder(request)
                                            .addAllMembers(selectedNodes)
                                            .build();
            raftConfigServiceFactory.getRaftConfigService().addContext(updatedContext);
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
    }

    @Override
    public void addNodeToContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        try {
            raftConfigServiceFactory.getRaftConfigService().addNodeToContext(request.getContext(),
                                                                             request.getNodeName());
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
    }

    @Override
    public void deleteNodeFromContext(NodeContext request, StreamObserver<Confirmation> responseObserver) {
        try {
            raftConfigServiceFactory.getRaftConfigService().deleteNodeFromContext(request.getContext(),
                                                                                  request.getNodeName());
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
    }

    @Override
    public void deleteContext(ContextName request, StreamObserver<Confirmation> responseObserver) {
        wrap(responseObserver,
             () -> raftConfigServiceFactory.getRaftConfigService().deleteContext(request.getContext()));
    }

    @Override
    public void createApplication(Application request, StreamObserver<Application> responseObserver) {

        try {
            Application response = raftConfigServiceFactory.getRaftConfigService().updateApplication(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
    }

    @Override
    public void deleteApplication(Application request, StreamObserver<Confirmation> responseObserver) {
        try {
            raftConfigServiceFactory.getRaftConfigService().deleteApplication(request);
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
    }

    @Override
    public void refreshToken(Application request, StreamObserver<Application> responseObserver) {
        try {
            Application response = raftConfigServiceFactory.getRaftConfigService().refreshToken(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
        }
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
}
