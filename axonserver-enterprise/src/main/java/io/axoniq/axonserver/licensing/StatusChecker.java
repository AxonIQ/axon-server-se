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

    public StatusChecker(LifecycleController lifecycleController) {
        this.lifecycleController = lifecycleController;
    }

    // not configurable, as must not be changed by customer
    @Scheduled(fixedRate = 3600000, initialDelay = 3600000)
    protected void checkLicense() {
        LicenseConfiguration.refresh();
        if( LicenseConfiguration.isEnterprise() && LocalDate.now().isAfter(LicenseConfiguration.getInstance().getExpiryDate())) {
            if( LocalDate.now().isBefore(LicenseConfiguration.getInstance().getGraceDate())) {
                log.warn("License has expired, AxonServer will continue working until {}", LicenseConfiguration.getInstance().getGraceDate());
            } else {
                lifecycleController.licenseError("AxonServer License has expired");
            }
        }

    }

}
