package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TransientException;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.springframework.stereotype.Component;

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

    @Override
    public void execute(Object payload) {
        try {
            AddNodeToContext addNodeToContext = (AddNodeToContext) payload;
            raftServiceFactory.getRaftConfigService().addNodeToContext(addNodeToContext.getContext(),
                                                                       clusterController.getName(),
                                                                       Role.PRIMARY);
        } catch (MessagingPlatformException ex) {
            if (ex.getErrorCode().equals(ErrorCode.NO_LEADER_AVAILABLE) ||
                    ex.getErrorCode().equals(ErrorCode.CONTEXT_UPDATE_IN_PROGRESS)) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }
}
