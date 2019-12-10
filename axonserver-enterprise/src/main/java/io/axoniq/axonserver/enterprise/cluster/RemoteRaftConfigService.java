package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Role;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.enterprise.CompetableFutureUtils.getFuture;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class RemoteRaftConfigService implements RaftConfigService {
    private static final Logger logger = LoggerFactory.getLogger(RemoteRaftConfigService.class);
    private static final Function<InstructionAck, Void> TO_VOID = x -> null;

    private final RaftConfigServiceGrpc.RaftConfigServiceStub raftConfigServiceStub;

    public RemoteRaftConfigService(RaftConfigServiceGrpc.RaftConfigServiceStub raftConfigServiceStub) {
        this.raftConfigServiceStub = raftConfigServiceStub;
    }

    @Override
    public void addNodeToContext(String context, String node, Role role) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.addNodeToContext(NodeContext.newBuilder()
                                                          .setNodeName(node)
                                                          .setRole(role)
                                                          .setContext(context)
                                                          .build(),
                                               new CompletableStreamObserver<>(completableFuture,
                                                                               "addNodeToContext",
                                                                               logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteNode(String name) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNode(NodeName.newBuilder().setNode(name).build(),
                                         new CompletableStreamObserver<>(completableFuture, "deleteNode", logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteContext(String context) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteContext(ContextName.newBuilder()
                                                       .setContext(context)
                                                       .build(),
                                            new CompletableStreamObserver<>(completableFuture,
                                                                            "deleteContext",
                                                                            logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteNodeFromContext(String context, String node) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNodeFromContext(NodeContext.newBuilder().setNodeName(node).setContext(context).build(),
                                                    new CompletableStreamObserver<>(completableFuture,
                                                                                    "deleteNodeFromContext",
                                                                                    logger));
        getFuture(completableFuture);
    }

    @Override
    public void addContext(String context, List<String> nodes) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.createContext(Context
                                                    .newBuilder()
                                                    .setName(context)
                                                    .addAllMembers(nodes.stream().map(n -> ContextMember.newBuilder().setNodeId(n).build()).collect(
                                                            Collectors.toList()))
                                                    .build(),
                                            new CompletableStreamObserver<>(completableFuture, "addContext", logger));
        getFuture(completableFuture);
    }

    @Override
    public void join(NodeInfo nodeInfo) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.joinCluster(nodeInfo, new CompletableStreamObserver<>(completableFuture, "join", logger));
        getFuture(completableFuture);
    }

    @Override
    public void init(List<String> contexts) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.initCluster(ContextNames.newBuilder().addAllContexts(contexts).build(),
                                          new CompletableStreamObserver<>(completableFuture, "init", logger));
        getFuture(completableFuture);

    }

    @Override
    public Application updateApplication(Application application) {
        CompletableFuture<Application> returnedApplication = new CompletableFuture<>();
        raftConfigServiceStub.updateApplication(application,
                                                new CompletableStreamObserver<>(returnedApplication,
                                                                                "updateApplication",
                                                                                logger));
        return getFuture(returnedApplication);
    }

    @Override
    public Application refreshToken(Application application) {
        CompletableFuture<Application> returnedApplication = new CompletableFuture<>();
        raftConfigServiceStub.refreshToken(application,
                                           new CompletableStreamObserver<>(returnedApplication,
                                                                           "refreshToken",
                                                                           logger));
        return getFuture(returnedApplication);
    }


    @Override
    public void updateUser(User request) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateUser(request,
                                         new CompletableStreamObserver<>(confirmation, "updateUser", logger, TO_VOID));
        getFuture(confirmation);
    }

    @Override
    public void updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateLoadBalanceStrategy(loadBalancingStrategy,
                                                        new CompletableStreamObserver<>(confirmation,
                                                                                        "updateLoadBalancingStrategy",
                                                                                        logger,
                                                                                        TO_VOID));
        getFuture(confirmation);
    }

    @Override
    public void deleteLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteLoadBalanceStrategy(loadBalancingStrategy,
                                                        new CompletableStreamObserver<>(confirmation,
                                                                                        "deleteLoadBalancingStrategy",
                                                                                        logger,
                                                                                        TO_VOID));
        getFuture(confirmation);
    }

    @Override
    public void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateProcessorLBStrategy(processorLBStrategy,
                                                        new CompletableStreamObserver<>(confirmation,
                                                                                        "updateProcessorLoadBalancing",
                                                                                        logger,
                                                                                        TO_VOID));
        getFuture(confirmation);
    }

    @Override
    public void deleteUser(User user) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteUser(user,
                                         new CompletableStreamObserver<>(confirmation, "deleteUser", logger, TO_VOID));
        getFuture(confirmation);
    }

    @Override
    public void deleteApplication(Application application) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteApplication(application,
                                                new CompletableStreamObserver<>(confirmation,
                                                                                "deleteApplication",
                                                                                logger,
                                                                                TO_VOID));
        getFuture(confirmation);
    }
}