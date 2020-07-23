package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task to start initialization of cluster with replication groups.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class InitAutoClusterTemplateTask implements ScheduledTask {

    private final RaftConfigServiceFactory raftConfigServiceFactory;
    private final Logger logger = LoggerFactory.getLogger(InitAutoClusterTemplateTask.class);
    private ClusterTemplate clusterTemplate;
    private final RegisterApplicationTask registerApplicationTask;
    private final RegisterUserTask registerUserTask;

    public InitAutoClusterTemplateTask(RaftConfigServiceFactory raftConfigServiceFactory,
                                       RegisterApplicationTask registerApplicationTask,
                                       RegisterUserTask registerUserTask) {

        this.raftConfigServiceFactory = raftConfigServiceFactory;

        this.registerApplicationTask = registerApplicationTask;
        this.registerUserTask = registerUserTask;
    }


    @Override
    public void execute(String context, Object payload) {

        initCluster();
        initApplications();
        initUsers();

    }

    private void initCluster() {
        raftConfigServiceFactory.getLocalRaftConfigService().init(Collections.emptyList());
    }


    @Autowired
    public void setClusterTemplate(ClusterTemplate clusterTemplate) {
        this.clusterTemplate = clusterTemplate;
    }

    private void initUsers() {
        clusterTemplate
                .getUsers()
                .forEach(user -> {
                            Set<UserRole> roles = buildUserRoles(user);
                            logger.info("Creating user:" + user.getUserName() + " with roles: " + roles);
                            registerUserTask.schedule(user.getUserName() , user.getPassword(), roles);
                        }
                );
    }

    @NotNull
    private Set<UserRole> buildUserRoles(ClusterTemplate.User user) {
        return user
                .getRoles()
                .stream()
                .flatMap(userRole -> userRole
                        .getRoles()
                        .stream()
                        .map(role -> new UserRole(userRole.getContext(), role)))
                .collect(Collectors.toSet());
    }

    private void initApplications() {
        clusterTemplate.getApplications().stream()
                .peek(app -> logger.info("Creating app:" + app))
                .forEach(registerApplicationTask::schedule);
    }

}
