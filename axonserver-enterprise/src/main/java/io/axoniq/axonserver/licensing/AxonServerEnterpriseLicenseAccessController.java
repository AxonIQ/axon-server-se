package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.LicenseAccessController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

/**
 * Check's and caches license validity flag for performant interceptor processing.
 * Updates validity flag occasionally and instantly once new license is updated.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Primary
@Controller
public class AxonServerEnterpriseLicenseAccessController implements LicenseAccessController {

    private final LicenseManager licenseManager;

    public AxonServerEnterpriseLicenseAccessController(LicenseManager licenseManager) {
        this.licenseManager = licenseManager;
        licenseIsValid = licenseManager.validateSilently();
    }

    private volatile boolean licenseIsValid;

    @Override
    public boolean allowed() {
        return licenseIsValid;
    }

    // not configurable, as must not be changed by customer
    @Scheduled(fixedRate = 3600000, initialDelay = 3600000)
    protected void checkLicense() {
        licenseIsValid = licenseManager.validateSilently();
    }

    @EventListener()
    public void on(ClusterEvents.LicenseUpdated licenseUpdated) {
        licenseIsValid = licenseManager.validateSilently(licenseUpdated.getLicense());
    }

}
