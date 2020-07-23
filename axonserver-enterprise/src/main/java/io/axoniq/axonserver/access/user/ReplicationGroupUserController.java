package io.axoniq.axonserver.access.user;

import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.axonserver.grpc.internal.UserContextRole;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Controls the user per context stored in the database. This information is replicated through raft to all nodes in a
 * the context.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Controller
public class ReplicationGroupUserController {

    private final ReplicationGroupUserRepository repository;

    public ReplicationGroupUserController(ReplicationGroupUserRepository repository) {
        this.repository = repository;
    }

    public void deleteUser(String context, String name) {
        repository.findByContextAndUsername(context, name)
                  .ifPresent(repository::delete);
    }

    public void mergeUser(String context, User user) {
        ReplicationGroupUser jpaContextUser = repository.findByContextAndUsername(context, user.getName())
                                                        .orElse(new ReplicationGroupUser(context, user.getName()));

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
        deleteByContext(contextDeleted.context());
    }


    public void deleteByContext(String context) {
        repository.findAllByContext(context).forEach(repository::delete);
    }

    public void save(ReplicationGroupUser userEntity) {
        repository.save(userEntity);
    }

    public Iterable<ReplicationGroupUser> getUsersForContexts(List<String> contextNames) {
        return repository.findAllByContextIn(contextNames);
    }
}
