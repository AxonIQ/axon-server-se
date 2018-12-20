package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.State;
import io.axoniq.axonserver.grpc.internal.User;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.enterprise.logconsumer.DeleteApplicationConsumer.DELETE_APPLICATION;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteLoadBalancingStrategyConsumer.DELETE_LOAD_BALANCING_STRATEGY;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteUserConsumer.DELETE_USER;

/**
 * Author: marc
 */
@Component
public class LocalRaftGroupService implements RaftGroupService {

    private GrpcRaftController grpcRaftController;

    public LocalRaftGroupService(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(String context, Node node) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.addNode(node);
    }

    @Override
    public void getStatus(Consumer<Context> contextConsumer) {
        grpcRaftController.raftGroups().forEach(name -> contextConsumer.accept(getStatus(name)));
    }

    @Override
    public CompletableFuture<Void> deleteNode(String context, String node) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.removeNode(node);
    }

    @Override
    public CompletableFuture<Void> initContext(String context, List<Node> raftNodes) {
        RaftGroup raftGroup = grpcRaftController.initRaftGroup(context);
        RaftNode leader = grpcRaftController.waitForLeader(raftGroup);
        raftNodes.forEach(n -> {
            try {
                leader.addNode(n).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        });

        return CompletableFuture.completedFuture(null);
    }

    public Context getStatus(String name) {
        RaftGroup raftGroup = grpcRaftController.getRaftGroup(name);
        RaftNode raftNode = raftGroup.localNode();
        String leader = raftNode.getLeader();
        return Context.newBuilder().setName(raftNode.groupId())
                      .addAllMembers(raftGroup.raftConfiguration()
                                              .groupMembers()
                                              .stream()
                                              .map(n -> ContextMember.newBuilder()
                                                                     .setNodeId(n.getNodeId())
                                                                     .setState(n.getNodeId()
                                                                                .equals(leader) ? State.LEADER : State.VOTING)
                                                                     .build())
                                              .collect(Collectors.toList())).build();
    }

    @Override
    public void stepDown(String context) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        if (raftNode != null && raftNode.isLeader()) {
            raftNode.stepdown();
        }
    }

    @Override
    public CompletableFuture<Void> updateApplication(String context, Application application) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(Application.class.getName(), application.toByteArray());
    }

    @Override
    public CompletableFuture<Void> updateUser(String context, User request) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(User.class.getName(), request.toByteArray());
    }

    @Override
    public CompletableFuture<Void> updateLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(LoadBalanceStrategy.class.getName(), loadBalancingStrategy.toByteArray());
    }

    @Override
    public CompletableFuture<Void> updateProcessorLoadBalancing(String context,
                                                                ProcessorLBStrategy processorLBStrategy) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray());
    }

    @Override
    public CompletableFuture<Void> deleteApplication(String context, Application application) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(DELETE_APPLICATION, application.toByteArray());
    }

    @Override
    public CompletableFuture<Void> deleteUser(String context, User request) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(DELETE_USER, request.toByteArray());
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(DELETE_LOAD_BALANCING_STRATEGY, loadBalancingStrategy.toByteArray());
    }

    @Override
    public CompletableFuture<Void> deleteContext(String context) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.removeGroup();
    }
}
