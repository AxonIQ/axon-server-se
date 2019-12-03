package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.taskscheduler.LocalTaskManager;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TransientException;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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

    private final LocalTaskManager localTaskManager;
    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RegisterNodeTask(LocalTaskManager localTaskManager,
                            ClusterController clusterController,
                            RaftConfigServiceFactory raftServiceFactory) {
        this.localTaskManager = localTaskManager;
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void execute(Object payload) {
        try {
            RegisterNodePayload registerNodePayload = (RegisterNodePayload) payload;
            NodeInfo nodeInfo = NodeInfo.newBuilder(clusterController.getMe().toNodeInfo())
                                        .addContexts(ContextRole.newBuilder().setName(CONTEXT_NONE))
                                        .build();

            raftServiceFactory.getRaftConfigServiceStub(registerNodePayload.getHostname(),
                                                        registerNodePayload.getPort())
                              .joinCluster(nodeInfo);

            registerNodePayload.contexts.forEach(context ->
                localTaskManager.createLocalTask(AddNodeToContextTask.class.getName(),
                                                 new AddNodeToContext(context),
                                                 Duration.ZERO));

        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode().equals(Status.Code.UNAVAILABLE)
                    || ex.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    public void schedule(String hostname, int port, List<String> contexts) {
        localTaskManager.createLocalTask(RegisterNodeTask.class.getName(),
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
