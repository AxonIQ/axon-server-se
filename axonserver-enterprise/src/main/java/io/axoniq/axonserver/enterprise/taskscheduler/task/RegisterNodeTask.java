package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

import static io.axoniq.axonserver.rest.ClusterRestController.CONTEXT_NONE;

/**
 * Task to start initialization of cluster. This task is scheduled for the other nodes in a cluster, if auto-cluster
 * properties are set.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class RegisterNodeTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(RegisterNodeTask.class);
    private final StandaloneTaskManager taskManager;
    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;
    private final ApplicationEventPublisher eventPublisher;

    public RegisterNodeTask(StandaloneTaskManager taskManager,
                            ClusterController clusterController,
                            RaftConfigServiceFactory raftServiceFactory,
                            ApplicationEventPublisher eventPublisher) {
        this.taskManager = taskManager;
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void execute(String context, Object payload) {
        try {
            RegisterNodePayload registerNodePayload = (RegisterNodePayload) payload;
            NodeInfo nodeInfo = NodeInfo.newBuilder(clusterController.getMe().toNodeInfo())
                                        .addContexts(ContextRole.newBuilder().setName(CONTEXT_NONE))
                                        .build();

            UpdateLicense updateLicense = raftServiceFactory.getRaftConfigServiceStub(registerNodePayload.getHostname(),
                    registerNodePayload.getPort())
                    .joinCluster(nodeInfo);

            eventPublisher.publishEvent(new ClusterEvents.LicenseUpdated(updateLicense.getLicense().toByteArray()));

            registerNodePayload.contexts.forEach(c ->
                    taskManager
                            .createTask(AddNodeToContextTask.class.getName(),
                                    new AddNodeToContext(c),
                                    Duration.ZERO));

        } catch (StatusRuntimeException ex) {
            logger.warn("Register node failed", ex);
            if (ex.getStatus().getCode().equals(Status.Code.UNAVAILABLE)
                    || ex.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    public void schedule(String hostname, int port, List<String> contexts) {
        taskManager.createTask(RegisterNodeTask.class.getName(),
                               new RegisterNodePayload(hostname, port, contexts),
                               Duration.ZERO);
    }

    @KeepNames
    public static class RegisterNodePayload {

        private String hostname;
        private int port;
        private List<String> contexts;

        public RegisterNodePayload() {
        }

        public RegisterNodePayload(String hostname, int port, List<String> contexts) {
            this.hostname = hostname;
            this.port = port;
            this.contexts = contexts;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public List<String> getContexts() {
            return contexts;
        }

        public void setContexts(List<String> contexts) {
            this.contexts = contexts;
        }
    }
}
