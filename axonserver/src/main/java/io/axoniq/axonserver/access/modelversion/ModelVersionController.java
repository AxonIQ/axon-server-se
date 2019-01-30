package io.axoniq.axonserver.access.modelversion;

import io.axoniq.axonserver.access.jpa.ApplicationModelVersion;
import org.springframework.stereotype.Controller;

import java.util.Collection;

/**
 * Keeps track of versions for access control entities. Versions are incremented with each update of the entity.
 *
 * @author Marc Gathier
 */
@Controller
public class ModelVersionController {
    private final ApplicationModelVersionRepository applicationModelVersionRepository;

    public ModelVersionController(
            ApplicationModelVersionRepository applicationModelVersionRepository) {
        this.applicationModelVersionRepository = applicationModelVersionRepository;
    }

    public long getModelVersion(Class<?> clazz) {
        return applicationModelVersionRepository.findById(clazz.getName())
                                                .map(ApplicationModelVersion::getVersion)
                                                .orElse(0L);
    }

    public void updateModelVersion(Class<?> clazz, long version) {
        ApplicationModelVersion applicationModelVersion =
                applicationModelVersionRepository.findById(clazz.getName())
                                                 .map(v -> {
                                                     v.setVersion(version);
                                                     return v;}
                                                 )
                                                 .orElse(new ApplicationModelVersion(clazz.getName(), version));
        applicationModelVersionRepository.save(applicationModelVersion);
    }

    public long incrementModelVersion(Class<?> clazz) {
        ApplicationModelVersion applicationModelVersion =
                applicationModelVersionRepository.findById(clazz.getName())
                                                 .map(v -> {
                                                     v.setVersion(v.getVersion()+1);
                                                     return v;}
                                                 )
                                                 .orElse(new ApplicationModelVersion(clazz.getName(), 1));
        applicationModelVersionRepository.save(applicationModelVersion);
        return applicationModelVersion.getVersion();
    }

    public Collection<ApplicationModelVersion> getModelVersions() {
        return applicationModelVersionRepository.findAll();
    }
}
