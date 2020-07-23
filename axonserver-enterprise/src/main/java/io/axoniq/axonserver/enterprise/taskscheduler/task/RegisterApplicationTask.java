package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Task to register application.
 * This task is scheduled for the other nodes in a cluster,
 * if cluster-template properties are set.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class RegisterApplicationTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(RegisterApplicationTask.class);
    private final StandaloneTaskManager taskManager;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RegisterApplicationTask(StandaloneTaskManager taskManager, RaftConfigServiceFactory raftServiceFactory) {
        this.taskManager = taskManager;

        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void execute(String context, Object payload) {
        try {
            RegisterApplicationTask.RegisterApplicationPayload registerApplicationPayload = (RegisterApplicationTask.RegisterApplicationPayload) payload;

            ClusterTemplate.Application application = registerApplicationPayload.getApplication();

            raftServiceFactory.getRaftConfigService()
                    .updateApplication(buildApplication(application));
        } catch (
                MessagingPlatformException ex) {
            logger.debug("Register application failed. Will try again", ex);
            if (ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode()) ||
                    ErrorCode.NO_LEADER_AVAILABLE.equals(ex.getErrorCode()) ||
                    ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(ex.getErrorCode())) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }

    public void schedule(ClusterTemplate.Application application) {
        taskManager.createTask(RegisterApplicationTask.class.getName(),
                new RegisterApplicationPayload(application),
                Duration.ZERO);
    }

    public Application buildApplication(ClusterTemplate.Application app) {
        Application.Builder builder = Application.newBuilder()
                .setName(app.getName())
                .setDescription(app.getDescription());
        builder.setToken(app.getToken());

        app.getRoles().stream()
                .map(this::buildApplicationContextRole)
                .forEach(builder::addRolesPerContext);

        return builder.build();
    }

    public ApplicationContextRole buildApplicationContextRole(ClusterTemplate.ApplicationRole role) {
        return ApplicationContextRole.newBuilder()
                .setContext(role.getContext())
                .addAllRoles(role.getRoles())
                .build();
    }

    @KeepNames
    public static class RegisterApplicationPayload {

        private ClusterTemplate.Application application;

        public ClusterTemplate.Application getApplication() {
            return application;
        }

        public void setApplication(ClusterTemplate.Application application) {
            this.application = application;
        }

        public RegisterApplicationPayload() {
        }

        public RegisterApplicationPayload(ClusterTemplate.Application application) {
            this.application = application;
        }

    }
}
