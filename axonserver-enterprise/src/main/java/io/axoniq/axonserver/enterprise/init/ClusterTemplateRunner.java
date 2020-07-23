package io.axoniq.axonserver.enterprise.init;

import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.taskscheduler.task.CreateReplicationGroupsAndContextsTask;
import io.axoniq.axonserver.enterprise.taskscheduler.task.InitAutoClusterTemplateTask;
import io.axoniq.axonserver.enterprise.taskscheduler.task.RegisterTemplateNodeTask;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Auto-initialize cluster based on cluster template configuration
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
@ConditionalOnProperty(name = "axoniq.axonserver.cluster-template.first")
public class ClusterTemplateRunner implements ApplicationRunner {

    private final ClusterTemplate clusterTemplate;
    private final ClusterController clusterController;
    private final InitAutoClusterTemplateTask initAutoClusterTemplateTask;
    private final RegisterTemplateNodeTask registerTemplateNodeTask;
    private final CreateReplicationGroupsAndContextsTask createReplicationGroupsAndContextsTask;
    private final Set<String> validRoles;

    private final Logger logger = LoggerFactory.getLogger(ClusterTemplateRunner.class);

    public ClusterTemplateRunner(ClusterTemplate clusterTemplate,
                                 ClusterController clusterController,
                                 RegisterTemplateNodeTask registerTemplateNodeTask,
                                 InitAutoClusterTemplateTask initAutoClusterTemplateTask,
                                 CreateReplicationGroupsAndContextsTask createReplicationGroupsAndContextsTask,
                                 RoleController roleController) {
        this.clusterTemplate = clusterTemplate;
        this.clusterController = clusterController;
        this.initAutoClusterTemplateTask = initAutoClusterTemplateTask;
        this.registerTemplateNodeTask = registerTemplateNodeTask;
        this.createReplicationGroupsAndContextsTask = createReplicationGroupsAndContextsTask;

        validRoles = roleController.listRoles()
                .stream()
                .map(Role::getRole)
                .collect(Collectors.toSet());
    }

    @Value("${axoniq.axonserver.autocluster.first:}")
    private String hasAutoClusterOn;

    private void initCluster() {

        String internalHostname = clusterTemplate.getFirst();

        int internalPort = MessagingPlatformConfiguration.DEFAULT_INTERNAL_GRPC_PORT;
        String[] firstNodeParts = internalHostname.split(":", 2);
        if (firstNodeParts.length > 1) {
            internalHostname = firstNodeParts[0];
            internalPort = Integer.parseInt(firstNodeParts[1]);
        }

        ClusterNode me = clusterController.getMe();
        if (me.getInternalHostName().equals(internalHostname) && me.getGrpcInternalPort().equals(internalPort)) {
            logger.info("This is the initial node for the cluster, already have {} contexts", me.getReplicationGroups().size());
            if (me.getReplicationGroups().isEmpty()) {

                logger.info("Starting Cluster Template...");

                validate();

                initAutoClusterTemplateTask.execute(getAdmin(), null);

                createReplicationGroupsAndContextsTask.schedule();
            }
        } else {
            logger.info(
                    "This {}:{} is not the initial node for the cluster, already have {} other nodes and {} contexts",
                    me.getInternalHostName(),
                    me.getGrpcInternalPort(),
                    clusterController.remoteNodeNames().size(),
                    me.getReplicationGroups().size());
            if (me.getReplicationGroups().isEmpty() && clusterController.remoteNodeNames().isEmpty()) {
                 logger.info("Starting Cluster Template...");

                 validate();
                 registerTemplateNodeTask.schedule(internalHostname, internalPort);
                createReplicationGroupsAndContextsTask.schedule();
            }
        }

    }

    private void validate(){
        validateUserRoles();
        validateApplicationRoles();
    }

    private void validateUserRoles() {
        clusterTemplate.getUsers()
                .stream()
                .flatMap(u-> u.getRoles().stream())
                .flatMap(userRole -> userRole
                        .getRoles()
                        .stream()
                        .map(role -> new UserRole(userRole.getContext(), role)))
                .forEach(role -> validateRole(role.getRole()));
    }


    private void validateApplicationRoles() {
        clusterTemplate.getApplications()
                .stream()
                .map(ClusterTemplate.Application::getRoles)
                .flatMap(List::stream)
                .map(ClusterTemplate.ApplicationRole::getRoles)
                .flatMap(List::stream)
                .distinct()
                .forEach(this::validateRole);
    }

    private void validateRole(String role) {
        if (!validRoles.contains(role)) {
            throw new MessagingPlatformException(ErrorCode.UNKNOWN_ROLE,
                    role);
        }
    }

    @Override
    public void run(ApplicationArguments args) {
        if(hasAutoClusterOn.isEmpty()) {
            initCluster();
        } else {
            logger.error("Not allowed to use 'Auto Cluster' and 'Cluster Template' simultaneously." +
                    "Please remove configuration: 'axoniq.axonserver.autocluster'");
        }
    }

}
