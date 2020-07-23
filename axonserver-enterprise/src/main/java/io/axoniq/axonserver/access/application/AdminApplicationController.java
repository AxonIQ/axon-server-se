package io.axoniq.axonserver.access.application;

import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author Marc Gathier
 */
@Controller
public class AdminApplicationController {

    private static final int PREFIX_LENGTH = 8;

    private final AdminApplicationRepository applicationRepository;
    private final Hasher hasher;

    public AdminApplicationController(AdminApplicationRepository applicationRepository, Hasher hasher) {
        this.applicationRepository = applicationRepository;
        this.hasher = hasher;
    }

    public List<AdminApplication> getApplications() {
        return applicationRepository.findAll();
    }

    /**
     * Updates the application with the given application. Updates token for application with the token in the
     * updatedApplication.
     * Does not activate listeners
     *
     * @param updatedApplication the new application definition
     */
    @Transactional
    public void synchronize(AdminApplication updatedApplication) {
        synchronized (applicationRepository) {
            AdminApplication application = applicationRepository.findFirstByName(updatedApplication.getName());
            if (application == null) {
                application = new AdminApplication(updatedApplication.getName(),
                                                   updatedApplication.getDescription(),
                                                   updatedApplication.getTokenPrefix(),
                                                   updatedApplication.getHashedToken());
                application.setMetaData(updatedApplication.getMetaData());
            } else {
                application.getContexts().clear();
                application.setHashedToken(updatedApplication.getHashedToken());
                application.setDescription(updatedApplication.getDescription());
            }
            final AdminApplication finalApplication = application;
            updatedApplication.getContexts().forEach(role -> finalApplication
                    .addContext(new AdminApplicationContext(role.getContext(), role.getRoles())));
            applicationRepository.save(application);
            applicationRepository.flush();
        }
    }

    public AdminApplication get(String name) {
        AdminApplication application = applicationRepository.findFirstByName(name);
        if (application == null) {
            throw new ApplicationNotFoundException(name);
        }
        return application;
    }

    public void delete(String name) {
        AdminApplication application = applicationRepository.findFirstByName(name);
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
        return token.substring(0, Math.min(token.length(), AdminApplicationController.PREFIX_LENGTH));
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
}
