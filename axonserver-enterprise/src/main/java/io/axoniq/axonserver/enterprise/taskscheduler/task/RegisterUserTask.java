package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Set;

import static io.axoniq.axonserver.exception.ErrorCode.CONTEXT_NOT_FOUND;

/**
 * Task to register user. This task is scheduled for the other nodes in a cluster, if cluster-template
 * properties are set.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class RegisterUserTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(RegisterUserTask.class);
    private final StandaloneTaskManager taskManager;
    private final UserControllerFacade userController;

    public RegisterUserTask(StandaloneTaskManager taskManager,
                            UserControllerFacade userController) {
        this.taskManager = taskManager;
        this.userController = userController;
    }

    @Override
    public void execute(String context, Object payload) {
        try {
            RegisterUserTask.RegisterUserPayload registerUserPayload = (RegisterUserTask.RegisterUserPayload) payload;

            userController.updateUser(registerUserPayload.getUsername(), registerUserPayload.getPassword(), registerUserPayload.getRoles());
        } catch (
                MessagingPlatformException ex) {
            logger.debug("Register user failed", ex);
            if (ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode()) ||
                    ErrorCode.NO_LEADER_AVAILABLE.equals(ex.getErrorCode()) ||
                    ErrorCode.CONTEXT_UPDATE_IN_PROGRESS.equals(ex.getErrorCode())) {
                throw new TransientException(ex.getMessage(), ex);
            } else {
                throw ex;
            }
        }
    }


    public void schedule(String username, String password, Set<UserRole> roles) {
        taskManager.createTask(RegisterUserTask.class.getName(),
                new RegisterUserPayload(username,password, roles),
                Duration.ZERO);
    }


    @KeepNames
    public static class RegisterUserPayload {

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        private String username;
        private String password;

        public Set<UserRole> getRoles() {
            return roles;
        }

        public void setRoles(Set<UserRole> roles) {
            this.roles = roles;
        }

        private Set<UserRole> roles;

        public RegisterUserPayload() {
        }

        public RegisterUserPayload(String username, String password, Set<UserRole> roles) {
            this.username = username;
            this.password = password;
            this.roles = roles;
        }
    }
}
