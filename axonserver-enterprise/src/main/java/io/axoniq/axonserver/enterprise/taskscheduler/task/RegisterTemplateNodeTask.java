package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static io.axoniq.axonserver.rest.ClusterRestController.CONTEXT_NONE;

/**
 * Task to start register a node with cluster template.
 * This task is scheduled for the other nodes in a cluster,
 * if cluster-template, properties are set.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class RegisterTemplateNodeTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(RegisterTemplateNodeTask.class);
    private final StandaloneTaskManager taskManager;
    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;
    private final ApplicationEventPublisher eventPublisher;

    public RegisterTemplateNodeTask(StandaloneTaskManager taskManager,
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
            RegisterTemplateNodePayload registerNodePayload = (RegisterTemplateNodePayload) payload;
            NodeInfo nodeInfo = NodeInfo.newBuilder(clusterController.getMe().toNodeInfo())
                    .addContexts(ContextRole.newBuilder().setName(CONTEXT_NONE))
                    .build();

            UpdateLicense updateLicense = raftServiceFactory.getRaftConfigServiceStub(registerNodePayload.getHostname(),
                    registerNodePayload.getPort())
                    .joinCluster(nodeInfo);

            eventPublisher.publishEvent(new ClusterEvents.LicenseUpdated(updateLicense.getLicense().toByteArray()));

        } catch (StatusRuntimeException ex) {
            logger.debug("Register node failed", ex);
            if (ex.getStatus().getCode().equals(Status.Code.UNAVAILABLE)
                    || ex.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    public void schedule(String hostname, int port) {
        taskManager.createTask(RegisterTemplateNodeTask.class.getName(),
                new RegisterTemplateNodePayload(hostname, port),
                Duration.ZERO);
    }

    @Autowired
    public void setClusterTemplate(ClusterTemplate clusterTemplate) {
    }

    @KeepNames
    public static class RegisterTemplateNodePayload {

        private String hostname;
        private int port;

        public RegisterTemplateNodePayload() {
        }

        public RegisterTemplateNodePayload(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
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

    }
}
