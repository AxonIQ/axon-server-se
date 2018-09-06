package io.axoniq.axonhub;

import io.axoniq.axonhub.licensing.LicenseConfiguration;
import io.axoniq.axonhub.licensing.LicenseException;
import io.axoniq.axonhub.rest.PluginImportSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * Author: marc
 */
@SpringBootApplication(scanBasePackages = "io.axoniq")
@EnableAsync
@EnableJpaRepositories("io.axoniq")
@EntityScan("io.axoniq")
@Import(PluginImportSelector.class)
public class AxonHubServer {
    private static final Logger log = LoggerFactory.getLogger(AxonHubServer.class);

    public static void main(String[] args) {
        try {
            LicenseConfiguration.getInstance();
        } catch(LicenseException ex) {
            log.error(ex.getMessage());
            System.exit(-1);
        }
        System.setProperty("spring.config.name", "axonhub");
        SpringApplication.run(AxonHubServer.class, args);
    }


}
