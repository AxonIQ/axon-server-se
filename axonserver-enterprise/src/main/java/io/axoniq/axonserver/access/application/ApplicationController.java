package io.axoniq.axonserver.access.application;

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
            throw new ApplicationNotFoundException(name);
        }
        applicationRepository.delete(application);
    }

    public String hash(String clearText) {
        return hasher.hash(clearText);
    }

    public void clearApplications() {
        applicationRepository.deleteAll();
    }

    public List<JpaApplication> getApplicationsForContext(String context) {
        return applicationRepository.findAllByContextsContext(context);
    }

    public void deleteByContext(String context) {
        synchronized (applicationRepository) {
            applicationRepository.deleteAllByContextsContext(context);
            applicationRepository.flush();
        }
    }

    public String tokenPrefix(String token) {
        if( token == null) return null;
        return token.substring(0, Math.min(token.length(), ApplicationController.PREFIX_LENGTH));
    }
}
