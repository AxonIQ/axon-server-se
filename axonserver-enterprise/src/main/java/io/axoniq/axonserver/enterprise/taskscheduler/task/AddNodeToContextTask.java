package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.TransientException;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 */
@Component
public class AddNodeToContextTask implements ScheduledTask {

    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;

    public AddNodeToContextTask(ClusterController clusterController,
                                RaftConfigServiceFactory raftServiceFactory) {
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
    }

    public CompletableFuture<Void> executeAsync(String context, Object payload) {
        AddNodeToContext addNodeToContext = (AddNodeToContext) payload;
        try {
            return doExecute(addNodeToContext);
        } catch (Exception ex) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(ex);
            return completableFuture;
        }
    }

    private CompletableFuture<Void> doExecute(AddNodeToContext addNodeToContext) {
        try {
            return raftServiceFactory.getRaftConfigService().addNodeToReplicationGroup(addNodeToContext.getContext(),
                                                                                       clusterController.getName(),
                                                                                       Role.PRIMARY)
                                     .exceptionally(this::checkTransient);
        } catch (MessagingPlatformException ex) {
            if (ErrorCode.NO_LEADER_AVAILABLE.equals(ex.getErrorCode()) ||
                    ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(ex.getErrorCode())) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    private Void checkTransient(Throwable ex) {
        if (ex instanceof MessagingPlatformException) {
            MessagingPlatformException mpe = (MessagingPlatformException) ex;
            if (ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(mpe.getErrorCode()) ||
                    ErrorCode.NO_LEADER_AVAILABLE.equals(mpe.getErrorCode())) {
                throw new TransientException(ex.getMessage(), ex);
            }
            throw (MessagingPlatformException) ex;
        }
        throw new RuntimeException(ex);
    }
}
