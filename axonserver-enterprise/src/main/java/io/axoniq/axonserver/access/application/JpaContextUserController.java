package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.axonserver.grpc.internal.UserContextRole;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Controller
public class JpaContextUserController {

    private final JpaContextUserRepository repository;

    public JpaContextUserController(JpaContextUserRepository repository) {
        this.repository = repository;
    }

    public void deleteUser(String context, String name) {
        repository.findByContextAndUsername(context, name)
                  .ifPresent(repository::delete);
    }

    public void mergeUser(String context, User user) {
        JpaContextUser jpaContextUser = repository.findByContextAndUsername(context, user.getName())
                                                  .orElse(new JpaContextUser(context, user.getName()));

        if (user.getRolesCount() == 0) {
            if (jpaContextUser.getId() != null) {
                repository.delete(jpaContextUser);
            }
        } else {
            jpaContextUser.setPassword(user.getPassword());

            jpaContextUser.setRoles(user.getRolesList().stream().map(UserContextRole::getRole)
                                        .collect(Collectors.toSet()));
            repository.save(jpaContextUser);
        }
    }

    @EventListener
    @Transactional
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        deleteByContext(contextDeleted.getContext());
    }


    public void deleteByContext(String context) {
        repository.findAllByContext(context).forEach(repository::delete);
    }

    public Iterable<JpaContextUser> getUsersForContext(String context) {
        return repository.findAllByContext(context);
    }

    public void save(JpaContextUser userEntity) {
        repository.save(userEntity);
    }
}
