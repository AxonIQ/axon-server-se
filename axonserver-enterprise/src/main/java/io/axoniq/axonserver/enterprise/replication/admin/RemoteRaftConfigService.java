package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
    public void createReplicationGroup(String name, Collection<ReplicationGroupMember> members) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.createReplicationGroup(ReplicationGroup.newBuilder()
                                                                     .setName(name)
                                                                     .addAllMembers(members)
                                                                     .build(),
                                                     new CompletableStreamObserver<>(completableFuture,
                                                                                     "createReplicationGroup",
                                                                                     logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteReplicationGroup(String name, boolean preserveEventStore) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteReplicationGroup(DeleteReplicationGroupRequest.newBuilder()
                                                                                  .setReplicationGroupName(name)
                                                                                  .setPreserveEventstore(
                                                                                          preserveEventStore)
                                                                                  .build(),
                                                     new CompletableStreamObserver<>(completableFuture,
                                                                                     "deleteReplicationGroup",
                                                                                     logger));
        getFuture(completableFuture);
    }

    @Override
    public CompletableFuture<Void> addNodeToReplicationGroup(String replicationGroup, String node, Role role) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.addNodeToReplicationGroup(NodeReplicationGroup.newBuilder()
                                                                            .setNodeName(node)
                                                                            .setReplicationGroupName(replicationGroup)
                                                                            .setRole(role)
                                                                            .build(),
                                                        new CompletableStreamObserver<>(completableFuture,
                                                                                        "addNodeToReplicationGroup",
                                                                                        logger,
                                                                                        TO_VOID));
        return completableFuture;
    }

    @Override
    public void deleteNode(String name) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNode(NodeName.newBuilder().setNode(name).build(),
                                         new CompletableStreamObserver<>(completableFuture, "deleteNode", logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteNodeIfEmpty(String name) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNode(NodeName.newBuilder().setNode(name).setRequireEmpty(true).build(),
                                         new CompletableStreamObserver<>(completableFuture, "deleteNode", logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteContext(String context, boolean preserveEventStore) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteContext(DeleteContextRequest.newBuilder()
                                                                .setContext(context)
                                                                .setPreserveEventstore(preserveEventStore)
                                                                .build(),
                                            new CompletableStreamObserver<>(completableFuture,
                                                                            "deleteContext",
                                                                            logger));
        getFuture(completableFuture);
    }

    @Override
    public void deleteNodeFromReplicationGroup(String replicationGroup, String node) {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNodeFromReplicationGroup(NodeReplicationGroup.newBuilder()
                                                                                 .setNodeName(node)
                                                                                 .setReplicationGroupName(
                                                                                         replicationGroup).build(),
                                                             new CompletableStreamObserver<>(completableFuture,
                                                                                             "deleteNodeFromReplicationGroup",
                                                                                             logger));
        getFuture(completableFuture);
    }

    @Override
    public CompletableFuture<Void> addContext(String replicationGroupName, String contextName,
                                              Map<String, String> metaData) {
        ReplicationGroupContext contextDefinition = ReplicationGroupContext.newBuilder()
                                                                           .setReplicationGroupName(replicationGroupName)
                                                                           .setContextName(contextName)
                                                                           .putAllMetaData(metaData)
                                                                           .build();
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.createContext(contextDefinition,
                                            new CompletableStreamObserver<>(completableFuture,
                                                                            "addContext",
                                                                            logger,
                                                                            TO_VOID));
        return completableFuture;
    }

    @Override
    public UpdateLicense join(NodeInfo nodeInfo) {
        CompletableFuture<UpdateLicense> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.joinCluster(nodeInfo, new CompletableStreamObserver<>(completableFuture, "join", logger));

        return getFuture(completableFuture);
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
        CompletableFuture<InstructionAck> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateUser(request,
                                         new CompletableStreamObserver<>(confirmation, "updateUser", logger));
        getFuture(confirmation);
    }

    @Override
    public void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        CompletableFuture<InstructionAck> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateProcessorLBStrategy(processorLBStrategy,
                                                        new CompletableStreamObserver<>(confirmation,
                                                                                        "updateProcessorLoadBalancing",
                                                                                        logger));
        getFuture(confirmation);
    }

    @Override
    public void deleteUser(User user) {
        CompletableFuture<InstructionAck> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteUser(user,
                                         new CompletableStreamObserver<>(confirmation, "deleteUser", logger));
        getFuture(confirmation);
    }

    @Override
    public void deleteApplication(Application application) {
        CompletableFuture<InstructionAck> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteApplication(application,
                                                new CompletableStreamObserver<>(confirmation,
                                                                                "deleteApplication",
                                                                                logger));
        getFuture(confirmation);
    }
}
