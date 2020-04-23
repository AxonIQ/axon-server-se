package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.LifecycleController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.time.LocalDate;

/**
 * @author Marc Gathier
 */
@Controller
public class StatusChecker {
    private final LifecycleController lifecycleController;
    private final Logger log = LoggerFactory.getLogger(StatusChecker.class);
    private final LicenseManager licenseManager;

    public StatusChecker(LifecycleController lifecycleController, LicenseManager licenseManager) {
        this.lifecycleController = lifecycleController;
        this.licenseManager = licenseManager;
    }

    // not configurable, as must not be changed by customer
    @Scheduled(fixedRate = 3600000, initialDelay = 3600000)
    protected void checkLicense() {
        LicenseConfiguration licenseConfiguration = licenseManager.getLicenseConfiguration();

        if( licenseConfiguration.isEnterprise() && LocalDate.now().isAfter(licenseConfiguration.getExpiryDate())) {
            if( LocalDate.now().isBefore(licenseConfiguration.getGraceDate())) {
                log.warn("License has expired, AxonServer will continue working until {}", licenseConfiguration.getGraceDate());
            } else {
                lifecycleController.licenseError("AxonServer License has expired");
            }
        }

    }

}
