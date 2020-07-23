package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Task to start initialization of cluster. This task is scheduled for the first node in a cluster, if auto-cluster
 * properties are set.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class InitClusterTask implements ScheduledTask {

    private final StandaloneTaskManager taskManager;
    private final RaftConfigServiceFactory raftConfigServiceFactory;

    public InitClusterTask(StandaloneTaskManager taskManager,
                           RaftConfigServiceFactory raftConfigServiceFactory) {
        this.taskManager = taskManager;
        this.raftConfigServiceFactory = raftConfigServiceFactory;
    }

    public void schedule(String[] contexts) {
        InitClusterPayload initClusterPayload = new InitClusterPayload(contexts);

        taskManager.createTask(InitClusterTask.class.getName(), initClusterPayload, Duration.ZERO);
    }

    @Override
    public void execute(String context, Object payload) {
        InitClusterPayload initClusterPayload = (InitClusterPayload) payload;
        List<String> contextList = Arrays.stream(initClusterPayload.getContexts())
                                         .filter(name -> !getAdmin().equals(name))
                                         .collect(Collectors.toList());

        raftConfigServiceFactory.getLocalRaftConfigService().init(contextList);
    }

    @KeepNames
    public static class InitClusterPayload {

        private String[] contexts;

        public InitClusterPayload() {
        }

        public InitClusterPayload(String[] contexts) {
            this.contexts = contexts;
        }

        public String[] getContexts() {
            return contexts;
        }

        public void setContexts(String[] contexts) {
            this.contexts = contexts;
        }
    }
}
