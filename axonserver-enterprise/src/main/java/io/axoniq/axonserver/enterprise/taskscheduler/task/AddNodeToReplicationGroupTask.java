package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.admin.LocalRaftConfigService;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Adds the current node to replication group with given role.
 * Used for Cluster Template.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class AddNodeToReplicationGroupTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(AddNodeToReplicationGroupTask.class);

    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;

    public AddNodeToReplicationGroupTask(ClusterController clusterController,
                                         RaftConfigServiceFactory raftServiceFactory) {
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
    }

    public CompletableFuture<Void> executeAsync(String context, Object payload) {
        AddNodeToReplicationGroup addNodeToReplicationGroup = (AddNodeToReplicationGroup) payload;
        try {
            return doExecute(addNodeToReplicationGroup);
        } catch (Exception ex) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(ex);
            return completableFuture;
        }
    }

    private CompletableFuture<Void> doExecute(AddNodeToReplicationGroup addNodeToReplicationGroup) {
        try {

            Role role = (addNodeToReplicationGroup.getRole() == null)
                    ? Role.PRIMARY : Role.valueOf(addNodeToReplicationGroup.getRole());

            try {
                ClusterNode me = clusterController.getMe();

                logger.info("Adding node '{}' to replication group '{}' with role '{}'"
                        , me.getInternalHostName()
                        , addNodeToReplicationGroup.getReplicationGroup()
                        , role);

                ReplicationGroupMember build = ReplicationGroupMember
                        .newBuilder()
                        .setNodeName(clusterController.getName())
                        .setHost(me.getHostName())
                        .setPort(me.getGrpcInternalPort())
                        .setRole(role)
                        .build();

                raftServiceFactory.getRaftConfigService()
                        .createReplicationGroup(addNodeToReplicationGroup.getReplicationGroup(), Collections.singletonList(build));

            } catch (MessagingPlatformException ex) {

                if (ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode()) ||
                        ErrorCode.NO_LEADER_AVAILABLE.equals(ex.getErrorCode()) ||
                        ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(ex.getErrorCode())) {
                    logger.debug("Unable to create replication group yet. Scheduled for retry.");
                    throw new TransientException(ex.getMessage(), ex);
                } else if (ErrorCode.CONTEXT_EXISTS.equals(ex.getErrorCode())) {
                    logger.debug("Replication group already exists, proceeding...");
                } else {
                    throw ex;
                }

            }

            CompletableFuture<Void> addNodeFuture = raftServiceFactory.getRaftConfigService()
                    .addNodeToReplicationGroup(addNodeToReplicationGroup.getReplicationGroup(), clusterController.getName(), role);

            if (addNodeFuture == null) {
                return CompletableFuture.completedFuture(null);
            } else {
                return addNodeFuture.exceptionally(this::checkTransient);
            }

        } catch (MessagingPlatformException ex) {
            if (ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode()) ||
                    ErrorCode.NO_LEADER_AVAILABLE.equals(ex.getErrorCode()) ||
                    ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(ex.getErrorCode())) {
                logger.debug("Node is not able to join replication group yet. Scheduled for retry.");
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    private Void checkTransient(Throwable ex) {
        if (ex instanceof MessagingPlatformException) {
            MessagingPlatformException mpe = (MessagingPlatformException) ex;
            if (ErrorCode.CONTEXT_NOT_FOUND.equals(mpe.getErrorCode()) ||
                    ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(mpe.getErrorCode()) ||
                    ErrorCode.NO_LEADER_AVAILABLE.equals(mpe.getErrorCode())) {
                logger.debug("Node is not able to join replication group yet. Scheduled for retry.");
                throw new TransientException(ex.getMessage(), ex);
            }
            throw (MessagingPlatformException) ex;
        }
        throw new RuntimeException(ex);
    }
}
