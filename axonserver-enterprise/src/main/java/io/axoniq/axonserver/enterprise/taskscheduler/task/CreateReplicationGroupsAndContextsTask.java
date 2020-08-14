package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.axonserver.rest.ClusterRestController.CONTEXT_NONE;

/**
 * Task to create replication groups and contexts.
 * This task is scheduled for the nodes in a cluster, if cluster-template properties are set.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class CreateReplicationGroupsAndContextsTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(CreateReplicationGroupsAndContextsTask.class);
    private final StandaloneTaskManager taskManager;
    private final ClusterController clusterController;
    private ClusterTemplate clusterTemplate;

    public CreateReplicationGroupsAndContextsTask(StandaloneTaskManager taskManager,
                                                  ClusterController clusterController) {
        this.taskManager = taskManager;
        this.clusterController = clusterController;
    }

    @Override
    public void execute(String context, Object payload) {
        try {
            NodeInfo nodeInfo = NodeInfo.newBuilder(clusterController.getMe().toNodeInfo())
                    .addContexts(ContextRole.newBuilder().setName(CONTEXT_NONE))
                    .build();

            createReplicationGroupsAndContexts(nodeInfo);

        } catch (StatusRuntimeException ex) {
            logger.debug("CreateReplicationGroupsAndContextsTask failed", ex);
            if (ex.getStatus().getCode().equals(Status.Code.UNAVAILABLE)
                    || ex.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    private void createReplicationGroupsAndContexts(NodeInfo nodeInfo) {
        clusterTemplate.getReplicationGroups()
                       .stream()
                       .filter(currentNodeConfiguration(nodeInfo))
                       .filter(hasAtLeastOnePrimaryNode())
                       .peek(rp -> addNodeToReplicationGroup(nodeInfo, rp))
                       .forEach(rp-> rp.getContexts().forEach(ctx-> {
                    addContextToReplicationGroup(rp, ctx.getName(), ctx.getMetaData());
                }));
    }

    private void addContextToReplicationGroup(ClusterTemplate.ReplicationsGroup rp, String ctx, Map<String,String> metaData) {
        taskManager
                .createTask(AddContextToReplicationGroupTask.class.getName(),
                        new AddContextToReplicationGroup(rp.getName(), ctx, metaData),
                        Duration.ZERO);
    }

    private void addNodeToReplicationGroup(NodeInfo nodeInfo, ClusterTemplate.ReplicationsGroup rp) {
        String role = rp.getRoles()
                .stream()
                .filter(r-> r.getNode().equals(nodeInfo.getNodeName()))
                .findFirst()
                .map(ClusterTemplate.ReplicationGroupRole::getRole)
                .orElse(Role.PRIMARY.name());

        taskManager
                .createTask(AddNodeToReplicationGroupTask.class.getName(),
                        new AddNodeToReplicationGroup(rp.getName(), role),
                        Duration.ZERO);
    }

    @NotNull
    private Predicate<ClusterTemplate.ReplicationsGroup> hasAtLeastOnePrimaryNode() {
        return rp -> rp.getRoles()
                .stream()
                .anyMatch(it -> it.getRole().equals(Role.PRIMARY.name()));
    }

    @NotNull
    private Predicate<ClusterTemplate.ReplicationsGroup> currentNodeConfiguration(NodeInfo nodeInfo) {
        return rp -> rp.getRoles()
                .stream()
                .anyMatch(role -> role.getNode().equals(nodeInfo.getNodeName()));
    }

    public void schedule() {
        taskManager.createTask(CreateReplicationGroupsAndContextsTask.class.getName(),
                "",
                Duration.ZERO);
    }

    @Autowired
    public void setClusterTemplate(ClusterTemplate clusterTemplate) {
        this.clusterTemplate = clusterTemplate;
    }

}
