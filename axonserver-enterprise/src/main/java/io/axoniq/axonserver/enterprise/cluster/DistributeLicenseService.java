package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.enterprise.taskscheduler.task.PrepareUpdateLicenseTask;
import io.axoniq.axonserver.licensing.LicenseManager;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Triggers distribution of license across cluster
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class DistributeLicenseService {

    private final TaskPublisher taskPublisher;

    private final LicenseManager licenseManager;

    public DistributeLicenseService(TaskPublisher taskPublisher, LicenseManager licenseManager) {
        this.taskPublisher = taskPublisher;
        this.licenseManager = licenseManager;
    }

    public void distributeLicense(byte[] licenseContent) {
        licenseManager.validate(licenseContent);

        //will work only if we initialize cluster, otherwise we cant upload license to initial node... to be discussed

        taskPublisher.publishScheduledTask(getAdmin(),
                PrepareUpdateLicenseTask.class
                        .getName(),
                licenseContent,
                Duration.ZERO);
    }


}
