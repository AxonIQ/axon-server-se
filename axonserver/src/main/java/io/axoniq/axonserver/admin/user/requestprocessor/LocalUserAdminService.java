package io.axoniq.axonserver.admin.user.requestprocessor;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public class LocalUserAdminService implements UserAdminService {

    private final UserController userController;
    private final ApplicationEventPublisher eventPublisher;

    public LocalUserAdminService(UserController userController,
                                 ApplicationEventPublisher eventPublisher) {
        this.userController = userController;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void createOrUpdateUser(@Nonnull String userName, @Nonnull String password, @Nonnull Set<? extends io.axoniq.axonserver.admin.user.api.UserRole> roles) {
        Set<UserRole> userRoles = roles.stream().map(r -> new UserRole(r.context(), r.role())).collect(Collectors.toSet());
        validateContexts(userRoles);
        User updatedUser = userController.updateUser(userName, password, userRoles);
        eventPublisher.publishEvent(new UserEvents.UserUpdated(updatedUser, false));
    }

    private void validateContexts(Set<UserRole> roles) {
        if (roles == null) {
            return;
        }
        if (roles.stream().anyMatch(userRole -> !validContext(userRole.getContext()))) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                    "Only specify context default for standard edition");
        }
    }

    private boolean validContext(String context) {
        return context == null || context.equals(Topology.DEFAULT_CONTEXT) || context.equals("*");
    }

    @Override
    public void deleteUser(@Nonnull String name) {
        userController.deleteUser(name);
        eventPublisher.publishEvent(new UserEvents.UserDeleted(name, false));
    }

    @Nonnull
    @Override
    public List<User> users() {
        return userController.getUsers();
    }
}
