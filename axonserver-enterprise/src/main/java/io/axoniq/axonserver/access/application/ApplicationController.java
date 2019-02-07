package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;


/**
 * @author Marc Gathier
 */
@Controller
public class ApplicationController {
    public static final int PREFIX_LENGTH = 8;

    private final ApplicationRepository applicationRepository;
    private final Hasher hasher;
    private final Map<String, Consumer<Application>> updateListeners = new ConcurrentHashMap<>();
    private final Map<String, Consumer<Application>> deleteListeners = new ConcurrentHashMap<>();


    public ApplicationController(ApplicationRepository applicationRepository, Hasher hasher) {
        this.applicationRepository = applicationRepository;
        this.hasher = hasher;
    }

    public List<Application> getApplications() {
        return applicationRepository.findAll();
    }

    public ApplicationWithToken updateToken(String name) {
        Application existingApplication = applicationRepository.findFirstByName(name);
        if( existingApplication == null) {
            throw new ApplicationNotFoundException(name);
        }

        String token = UUID.randomUUID().toString();
        existingApplication.setHashedToken(hasher.hash(token));
        existingApplication.setTokenPrefix(token.substring(0, PREFIX_LENGTH));
        updateListeners.forEach((key, listener) -> listener.accept(existingApplication));
        return new ApplicationWithToken(token, existingApplication);
    }

    /**
     * Updates the application with the given application.
     * If if is a new application it generates a new application token
     * For an existing application it does not override the existing token
     * Calls listeners
     * @param application
     * @return application with generated token or message "Token already returned"
     */
    public ApplicationWithToken updateJson(Application application) {
        Application existingApplication = applicationRepository.findFirstByName(application.getName());
        String token = "Token already returned";
        if( existingApplication == null) {
            token = getOrDefault(application.getHashedToken(), UUID.randomUUID().toString());
            application.setTokenPrefix(token.substring(0, Math.min(PREFIX_LENGTH, token.length())));
            existingApplication = new Application(application.getName(), application.getDescription(), application.getTokenPrefix(), hasher.hash(token));
            existingApplication = applicationRepository.save(existingApplication);
        } else {
            existingApplication.getContexts().clear();
            existingApplication.setDescription(application.getDescription());

        }
        final Application finalApplication = existingApplication;
        application.getContexts().forEach(role -> finalApplication.addContext(new ApplicationContext(role.getContext(), role.getRoles())));
        applicationRepository.save(finalApplication);
        updateListeners.forEach((key, listener) -> listener.accept(finalApplication));
        return new ApplicationWithToken(token, finalApplication);
    }


    /**
     * Updates the application with the given application. Updates token for application with the token in the updatedApplication.
     * Does not activate listeners
     * @param updatedApplication
     */
    @Transactional
    public void synchronize(Application updatedApplication) {
        synchronize(updatedApplication, true);
    }


    /**
     * Updates application with given application.
     * @param updatedApplication update application
     */
    @Transactional
    public void synchronize(Application updatedApplication, boolean synchronizeRoles) {
        synchronized (applicationRepository) {
            Application application = applicationRepository.findFirstByName(updatedApplication.getName());
            if (application == null) {
                application = new Application(updatedApplication.getName(),
                                              updatedApplication.getDescription(),
                                              updatedApplication.getTokenPrefix(),
                                              updatedApplication.getHashedToken());
            } else {
                if (synchronizeRoles) application.getContexts().clear();
                application.setHashedToken(updatedApplication.getHashedToken());
                application.setDescription(updatedApplication.getDescription());
            }
            final Application finalApplication = application;
            if (synchronizeRoles) updatedApplication.getContexts().forEach(role -> finalApplication
                    .addContext(new ApplicationContext(role.getContext(), role.getRoles())));
            applicationRepository.save(application);
            applicationRepository.flush();
        }
    }

    public Application get(String name) {
        Application application = applicationRepository.findFirstByName(name);
        if (application == null) {
            throw new ApplicationNotFoundException(name);
        }
        return application;
    }

    public void delete( String name) {
        Application application = applicationRepository.findFirstByName(name);
        if (application == null) {
            throw new ApplicationNotFoundException(name);
        }
        applicationRepository.delete(application);
        deleteListeners.forEach((key, deleteListener) -> deleteListener.accept(application));
    }

    public void registerUpdateListener(String name, Consumer<Application> updateListener) {
        updateListeners.put(name, updateListener);
    }

    public void registerDeleteListener(String name, Consumer<Application> deleteListener) {
        deleteListeners.put(name, deleteListener);
    }

    public void deregisterListeners(String name) {
        updateListeners.remove(name);
        deleteListeners.remove(name);
    }

    public String hash(String clearText) {
        return hasher.hash(clearText);
    }


    public void clearApplications() {
        applicationRepository.deleteAll();
    }

    @Transactional
    public void mergeContext(Application update, String context) {
        synchronized(applicationRepository) {
            Application application = applicationRepository.findFirstByName(update.getName());
            if (application == null) {
                if (update.getContexts().isEmpty()) return;

                applicationRepository.save(update);
                return;
            }

            application.setDescription(update.getDescription());
            if (!org.springframework.util.StringUtils.isEmpty(update.getTokenPrefix())) {
                application.setHashedToken(update.getHashedToken());
            }
            if (!org.springframework.util.StringUtils.isEmpty(update.getTokenPrefix())) {
                application.setTokenPrefix(update.getTokenPrefix());
            }

            application.removeContext(context);
            update.getContexts().forEach(application::addContext);
            if( application.getContexts().isEmpty()) {
                applicationRepository.delete(application);
            }
            applicationRepository.flush();
        }
    }

    public List<Application> getApplicationsForContext(String context) {
        return applicationRepository.findAllByContextsContext(context);
    }

    public void deleteByContext(String context) {
        synchronized (applicationRepository) {
            applicationRepository.deleteAllByContextsContext(context);
            applicationRepository.flush();
        }
    }
}
