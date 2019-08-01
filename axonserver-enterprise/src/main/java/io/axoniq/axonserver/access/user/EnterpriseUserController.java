package io.axoniq.axonserver.access.user;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.enterprise.ContextEvents;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Manages {@link User}'s operations in AxonServer enterprise edition.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@Component
public class EnterpriseUserController {

    private final UserRepository userRepository;

    public EnterpriseUserController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    /**
     * Listen for {@link io.axoniq.axonserver.enterprise.ContextEvents.AdminContextDeleted}. If this event occurs clear
     * all {@link User} entries as I am no longer an admin node.
     *
     * @param adminContextDeleted event raised
     */
    @EventListener
    public void on(ContextEvents.AdminContextDeleted adminContextDeleted) {
        userRepository.deleteAll();
    }
}
