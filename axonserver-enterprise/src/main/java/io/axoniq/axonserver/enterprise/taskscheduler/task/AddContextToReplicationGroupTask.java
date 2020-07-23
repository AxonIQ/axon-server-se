package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 *
 * Adds context to replication group. Used in Cluster Template.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class AddContextToReplicationGroupTask implements ScheduledTask {

    private final RaftConfigServiceFactory raftServiceFactory;

    public AddContextToReplicationGroupTask(RaftConfigServiceFactory raftServiceFactory) {
        this.raftServiceFactory = raftServiceFactory;
    }

    public CompletableFuture<Void> executeAsync(String context, Object payload) {
        AddContextToReplicationGroup addContextToReplicationGroup = (AddContextToReplicationGroup) payload;
        try {
            return doExecute(addContextToReplicationGroup);
        } catch (Exception ex) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(ex);
            return completableFuture;
        }
    }

    private CompletableFuture<Void> doExecute(AddContextToReplicationGroup addContextToReplicationGroup) {
        try {

            CompletableFuture<Void> addContextFuture = raftServiceFactory.getRaftConfigService().addContext(addContextToReplicationGroup.getReplicationGroup(),
                    addContextToReplicationGroup.getContext(), addContextToReplicationGroup.getContextMetaData());

            if (addContextFuture == null) {
                return CompletableFuture.completedFuture(null);
            } else {
                return addContextFuture.exceptionally(this::checkTransient);
            }
        } catch (MessagingPlatformException ex) {
            if ( ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode()) ||
                    ErrorCode.NO_LEADER_AVAILABLE.equals(ex.getErrorCode()) ||
                    ErrorCode.OTHER.equals(ex.getErrorCode()) || //transfer leadership
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
            if (ErrorCode.CONTEXT_NOT_FOUND.equals(mpe.getErrorCode()) ||
                    ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(mpe.getErrorCode()) ||
                    ErrorCode.OTHER.equals(mpe.getErrorCode()) || //transfer leadership
                    ErrorCode.NO_LEADER_AVAILABLE.equals(mpe.getErrorCode())) {
                throw new TransientException(ex.getMessage(), ex);
            }
            throw (MessagingPlatformException) ex;
        }
        throw new RuntimeException(ex);
    }
}
