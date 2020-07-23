package io.axoniq.axonserver.enterprise.replication.admin;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupService;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class FakeRaftGroupService implements RaftGroupService {

    private Map<String, String> leaders = new HashMap<>();
    private Map<String, List<SerializedObject>> logEntries = new HashMap<>();
    private Map<String, ReplicationGroup> replicationGroups = new HashMap<>();

    @Override
    public CompletableFuture<ReplicationGroupUpdateConfirmation> addServer(String replicationGroup, Node node) {
        ReplicationGroup adminReplicationGroup = ReplicationGroup.newBuilder(replicationGroups.get(replicationGroup))
                                                                 .addMembers(ReplicationGroupMember.newBuilder()
                                                                                                   .setNodeName(node.getNodeName())
                                                                                                   .setRole(node.getRole())
                                                                                                   .build()).build();
        replicationGroups.put(replicationGroup, adminReplicationGroup);
        return CompletableFuture.completedFuture(ReplicationGroupUpdateConfirmation.newBuilder()
                                                                                   .setSuccess(true)
                                                                                   .addAllMembers(adminReplicationGroup
                                                                                                          .getMembersList())
                                                                                   .build());
    }

    @Override
    public CompletableFuture<Void> getStatus(Consumer<ReplicationGroup> replicationGroupConsumer) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ReplicationGroupConfiguration> initReplicationGroup(String replicationGroupName,
                                                                                 List<Node> nodes) {
        ReplicationGroup replicationGroup = ReplicationGroup.newBuilder()
                                                            .setName(replicationGroupName)
                                                            .addAllMembers(nodes.stream()
                                                                                .map(n -> ReplicationGroupMember
                                                                                        .newBuilder()
                                                                                        .setNodeName(n.getNodeName())
                                                                                        .build())
                                                                                .collect(Collectors.toList()))
                                                            .build();
        replicationGroups.put(replicationGroupName, replicationGroup);
        ReplicationGroupConfiguration configuration =
                ReplicationGroupConfiguration.newBuilder()
                                             .setReplicationGroupName(replicationGroupName)
                                             .addAllNodes(nodes.stream()
                                                               .map(node -> NodeInfoWithLabel
                                                                       .newBuilder()
                                                                       .setNode(NodeInfo.newBuilder()
                                                                                        .setNodeName(node.getNodeName()))
                                                                       .build())
                                                               .collect(
                                                                       Collectors
                                                                               .toList()))
                                             .build();
        return CompletableFuture.completedFuture(configuration);
    }

    @Override
    public CompletableFuture<ReplicationGroupUpdateConfirmation> deleteServer(String replicationGroupName,
                                                                              String node) {
        ReplicationGroup old = replicationGroups.get(replicationGroupName);
        ReplicationGroup replicationGroup = ReplicationGroup.newBuilder(old)
                                                            .clearMembers()
                                                            .addAllMembers(old.getMembersList()
                                                                              .stream()
                                                                              .filter(m -> !m.getNodeName()
                                                                                             .equals(node))
                                                                              .collect(Collectors.toList()))
                                                            .build();
        replicationGroups.put(replicationGroupName, replicationGroup);
        ReplicationGroupUpdateConfirmation confirmation = ReplicationGroupUpdateConfirmation.newBuilder().setSuccess(
                true).addAllMembers(replicationGroup.getMembersList()).build();
        return CompletableFuture.completedFuture(confirmation);
    }

    @Override
    public CompletableFuture<Void> deleteReplicationGroup(String replicationGroup, boolean preserveEventStore) {
        replicationGroups.remove(replicationGroup);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> appendEntry(String replicationGroup, String name, byte[] bytes) {
        SerializedObject serializedObject = SerializedObject.newBuilder()
                                                            .setType(name)
                                                            .setData(ByteString.copyFrom(bytes))
                                                            .build();
        logEntries.computeIfAbsent(replicationGroup, r -> new ArrayList<>()).add(serializedObject);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ReplicationGroupConfiguration> configuration(String replicationGroupName) {
        ReplicationGroup replicationGroup = replicationGroups.get(replicationGroupName);
        ReplicationGroupConfiguration configuration =
                ReplicationGroupConfiguration.newBuilder()
                                             .setReplicationGroupName(replicationGroupName)
                                             .addAllNodes(replicationGroup.getMembersList()
                                                                          .stream()
                                                                          .map(node -> NodeInfoWithLabel
                                                                                  .newBuilder()
                                                                                  .setNode(NodeInfo.newBuilder()
                                                                                                   .setNodeName(node.getNodeName()))
                                                                                  .build())
                                                                          .collect(
                                                                                  Collectors
                                                                                          .toList()))
                                             .build();
        return CompletableFuture.completedFuture(configuration);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String replicationGroup) {
        replicationGroups.getOrDefault(replicationGroup, ReplicationGroup.getDefaultInstance())
                         .getMembersList()
                         .stream()
                         .filter(m -> !m.getNodeName().equals(leaders.get(replicationGroup)))
                         .findFirst().ifPresent(m -> leaders.put(replicationGroup, m.getNodeName()));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> prepareDeleteNodeFromReplicationGroup(String replicationGroup, String node) {
        return CompletableFuture.completedFuture(null);
    }

    public List<SerializedObject> logEntries(String replicationGroup) {
        return logEntries.getOrDefault(replicationGroup, Collections.emptyList());
    }

    public void addReplicationGroup(String replicationGroupName, String... nodes) {
        ReplicationGroup replicationGroup = ReplicationGroup.newBuilder()
                                                            .setName(replicationGroupName)
                                                            .addAllMembers(Arrays.stream(nodes)
                                                                                 .map(n -> ReplicationGroupMember
                                                                                         .newBuilder()
                                                                                         .setNodeName(n)
                                                                                         .build())
                                                                                 .collect(Collectors.toList()))
                                                            .build();
        replicationGroups.put(replicationGroupName, replicationGroup);
    }

    public ReplicationGroupMember groupNode(String replicationGroup, String node) {
        return replicationGroups.getOrDefault(replicationGroup, ReplicationGroup.getDefaultInstance())
                                .getMembersList()
                                .stream()
                                .filter(n -> n.getNodeName().equals(node))
                                .findFirst().orElse(null);
    }

    public String getLeader(String replicationGroup) {
        return leaders.get(replicationGroup);
    }

    public void setLeader(String replicationGroup, String node) {
        leaders.put(replicationGroup, node);
    }
}
