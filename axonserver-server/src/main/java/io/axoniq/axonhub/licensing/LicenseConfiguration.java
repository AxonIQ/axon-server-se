package io.axoniq.axonhub.licensing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;

/**
 * Singleton that provides access to information from the license key used by this
 * instance of the Messaging Platform, in particular the effective rate limit.
 *
 * This is deliberately not a Spring component, since all of the flexibility that Spring
 * offers to choose different implementations and override properties, would just be
 * a Bad Thing here.
 *
 * Initial call to getInstance() should take place in main() before Boot starts. As a
 * result, instance initialization doesn't need to be thread safe.
 *
 * Actual complexity of reading/verifying is delegated to the LicensePropertyReader.
 *
 * @author Frans van Buul
 *
 */
public final class LicenseConfiguration {

    private static final String AXON_HUB = "AxonHub";

    public enum Edition {
        Enterprise, Free
    }
    private static final Logger log = LoggerFactory.getLogger(LicenseConfiguration.class);
    private static LicenseConfiguration instance;

    public static LicenseConfiguration getInstance() {
        if(instance == null) {
            Properties properties = new LicensePropertyReader().readLicenseProperties();
            if(properties == null) {
                log.warn("License property not specified - Running in Free mode");
                instance = new LicenseConfiguration(null, Edition.Free, UUID.randomUUID().toString(),
                        1, 1, null);
            } else {
                instance = new LicenseConfiguration(
                        LocalDate.parse(properties.getProperty("expiry_date")),
                        Edition.valueOf(properties.getProperty("edition")),
                        properties.getProperty("license_key_id"),
                        Integer.valueOf(properties.getProperty("contexts", "1")),
                        Integer.valueOf(properties.getProperty("clusterNodes", "3")),
                        properties.getProperty("licensee"));
                if(LocalDate.now().isAfter(instance.expiryDate))
                    throw LicenseException.expired(instance.expiryDate);
                if(!validProduct(properties.getProperty("product"))) {
                    throw LicenseException.wrongProduct(AXON_HUB);
                }
                log.info("Licensed to: {}", instance.licensee);
                log.info("Running {} mode", instance.edition);
                log.info("License expiry date is {}", instance.expiryDate);
            }
        }
        return instance;
    }

    private static boolean validProduct(String product) {
        return product != null && product.contains(AXON_HUB);
    }

    private final LocalDate expiryDate;
    private final Edition edition;
    private final String licenseId;
    private final int contexts;
    private final int clusterNodes;
    private final String licensee;

    LicenseConfiguration(LocalDate expiryDate, Edition edition, String licenseId, int contexts, int clusterNodes, String licensee) {
        this.expiryDate = expiryDate;
        this.edition = edition;
        this.licenseId = licenseId;
        this.contexts = contexts;
        this.clusterNodes = clusterNodes;
        this.licensee = licensee;
    }

    public LocalDate getExpiryDate() {
        return expiryDate;
    }

    public Edition getEdition() {
        return edition;
    }

    public int getContexts() {
        return contexts;
    }

    public String getLicenseId() {
        return licenseId;
    }

    public String getLicensee() {
        return licensee;
    }

    public int getClusterNodes() {
        return clusterNodes;
    }

    public static boolean isDeveloper() {
        return Edition.Free.equals(getInstance().edition);
    }

    public static boolean isEnterprise() {
        return Edition.Enterprise.equals(getInstance().edition);
    }
}
