package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.ApplicationModelVersion;
import org.springframework.stereotype.Controller;

import java.util.Collection;

/**
 * @author Marc Gathier
 */
@Controller
public class ApplicationModelController {
    public static final int PREFIX_LENGTH = 8;

    private final ApplicationModelVersionRepository applicationModelVersionRepository;


    public ApplicationModelController( ApplicationModelVersionRepository applicationModelVersionRepository) {
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
