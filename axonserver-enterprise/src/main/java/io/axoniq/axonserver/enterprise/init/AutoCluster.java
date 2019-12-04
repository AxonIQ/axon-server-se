package io.axoniq.axonserver.enterprise.init;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.taskscheduler.task.InitClusterTask;
import io.axoniq.axonserver.enterprise.taskscheduler.task.RegisterNodeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Auto-initialize cluster based on properties
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
@ConditionalOnProperty(name = "axoniq.axonserver.autocluster.first")
public class AutoCluster implements ApplicationRunner {

    private final AtomicBoolean running = new AtomicBoolean();
    private final Logger logger = LoggerFactory.getLogger(AutoCluster.class);

    private final String firstNode;
    private final String[] contexts;
    private final ClusterController clusterController;
    private final InitClusterTask initClusterTask;
    private final RegisterNodeTask registerNodeTask;

    public AutoCluster(@Value("${axoniq.axonserver.autocluster.first}") String firstNode,
                       @Value("${axoniq.axonserver.autocluster.contexts:#{null}}") String[] contexts,
                       ClusterController clusterController,
                       InitClusterTask initClusterTask,
                       RegisterNodeTask registerNodeTask) {
        this.firstNode = firstNode;
        this.contexts = contexts == null ? new String[0] : contexts;
        this.clusterController = clusterController;
        this.initClusterTask = initClusterTask;
        this.registerNodeTask = registerNodeTask;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("Starting auto-clustering with first node {} and contexts {}", firstNode, contexts);

        String internalHostname = firstNode;
        int internalPort = MessagingPlatformConfiguration.DEFAULT_INTERNAL_GRPC_PORT;
        String[] firstNodeParts = firstNode.split(":", 2);
        if (firstNodeParts.length > 1) {
            internalHostname = firstNodeParts[0];
            internalPort = Integer.parseInt(firstNodeParts[1]);
        }

        ClusterNode me = clusterController.getMe();
        if (me.getInternalHostName().equals(internalHostname) && me.getGrpcInternalPort().equals(internalPort)) {
            logger.info("This is the initial node for the cluster, already have {} contexts", me.getContexts().size());
            if (me.getContexts().isEmpty()) {
                initClusterTask.schedule(contexts);
            }
        } else {
            logger.info(
                    "This {}:{} is not the initial node for the cluster, already have {} other nodes and {} contexts",
                    me.getInternalHostName(),
                    me.getGrpcInternalPort(),
                    clusterController.remoteNodeNames().size(),
                    me.getContexts().size());
            if (me.getContexts().isEmpty() && clusterController.remoteNodeNames().isEmpty()) {
                registerNodeTask.schedule(internalHostname, internalPort, Arrays.asList(contexts));
            }
        }
    }
}
