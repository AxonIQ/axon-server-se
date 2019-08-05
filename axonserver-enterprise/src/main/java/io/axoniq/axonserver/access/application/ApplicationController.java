package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.enterprise.ContextEvents;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author Marc Gathier
 */
@Controller
public class ApplicationController {
    private static final int PREFIX_LENGTH = 8;

    private final JpaApplicationRepository applicationRepository;
    private final Hasher hasher;

    public ApplicationController(JpaApplicationRepository applicationRepository, Hasher hasher) {
        this.applicationRepository = applicationRepository;
        this.hasher = hasher;
    }

    public List<JpaApplication> getApplications() {
        return applicationRepository.findAll();
    }

    /**
     * Updates the application with the given application. Updates token for application with the token in the updatedApplication.
     * Does not activate listeners
     * @param updatedApplication the new application definition
     */
    @Transactional
    public void synchronize(JpaApplication updatedApplication) {
        synchronized (applicationRepository) {
            JpaApplication application = applicationRepository.findFirstByName(updatedApplication.getName());
            if (application == null) {
                application = new JpaApplication(updatedApplication.getName(),
                                              updatedApplication.getDescription(),
                                              updatedApplication.getTokenPrefix(),
                                              updatedApplication.getHashedToken());
            } else {
                application.getContexts().clear();
                application.setHashedToken(updatedApplication.getHashedToken());
                application.setDescription(updatedApplication.getDescription());
            }
            final JpaApplication finalApplication = application;
            updatedApplication.getContexts().forEach(role -> finalApplication
                    .addContext(new ApplicationContext(role.getContext(), role.getRoles())));
            applicationRepository.save(application);
            applicationRepository.flush();
        }
    }

    public JpaApplication get(String name) {
        JpaApplication application = applicationRepository.findFirstByName(name);
        if (application == null) {
            throw new ApplicationNotFoundException(name);
        }
        return application;
    }

    public void delete( String name) {
        JpaApplication application = applicationRepository.findFirstByName(name);
        if (application == null) {
            return;
        }
        applicationRepository.delete(application);
    }

    public String hash(String clearText) {
        return hasher.hash(clearText);
    }

    @Transactional
    public void clearApplications() {
        applicationRepository.deleteAll();
    }

    public static String tokenPrefix(String token) {
        if( token == null) return null;
        return token.substring(0, Math.min(token.length(), ApplicationController.PREFIX_LENGTH));
    }

    /**
     * Removes all roles for {@code context} from all applications.
     *
     * @param context the context to remove
     */
    @Transactional
    public void removeRolesForContext(String context) {
        applicationRepository.findAllByContextsContext(context).forEach(app -> app.removeContext(context));
    }

    /**
     * Listen for {@link io.axoniq.axonserver.enterprise.ContextEvents.AdminContextDeleted}. If this event occurs clear
     * all {@link JpaApplication} entries as I am no longer an admin node.
     *
     * @param adminContextDeleted event raised
     */
    @EventListener
    public void on(ContextEvents.AdminContextDeleted adminContextDeleted) {
        applicationRepository.deleteAll();
    }
}
