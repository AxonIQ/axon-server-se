package io.axoniq.axonserver.licensing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    private static final String AXON_SERVER = "AxonServer";

    public static void refresh() {
        Properties properties = new LicensePropertyReader().readLicenseProperties();
        if(properties == null) {
            if( ! Edition.Free.equals(instance.edition)) {
                instance = instance.withGraceDate( LocalDate.now().minus(1, ChronoUnit.DAYS));
            }
        } else {
            instance = instance.withGraceDate( getLocalDate(properties.getProperty("grace_date")))
            .withExpiryDate(getLocalDate(properties.getProperty("expiry_date")));
        }
    }


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
                                                    1, 1, null,
                                                    AXON_SERVER,
                                                    null,
                                                    null);
            } else {
                instance = new LicenseConfiguration(
                        getLocalDate(properties.getProperty("expiry_date")),
                        Edition.Enterprise,
                        properties.getProperty("license_key_id"),
                        Integer.valueOf(properties.getProperty("contexts", "1")),
                        Integer.valueOf(properties.getProperty("clusterNodes", "3")),
                        properties.getProperty("licensee"),
                        properties.getProperty("product"),
                        properties.getProperty("packs"),
                        getLocalDate(properties.getProperty("grace_date")));
                if(LocalDate.now().isAfter(instance.expiryDate)) {
                    if( LocalDate.now().isBefore(instance.graceDate)) {
                        log.warn("License has expired, AxonServer will continue working until {}", instance.graceDate);
                    } else {
                        throw LicenseException.expired(instance.expiryDate);
                    }
                }
                if(!validProduct(properties.getProperty("product"))) {
                    throw LicenseException.wrongProduct(AXON_SERVER);
                }
                log.info("Licensed to: {}", instance.licensee);
                log.info("Running {} mode", instance.edition);
                log.info("License expiry date is {}", instance.expiryDate);
            }
        }
        return instance;
    }

    private static LocalDate getLocalDate(String dateString) {
        if(StringUtils.isEmpty(dateString)) return null;
        return LocalDate.parse(dateString);

    }

    private static boolean validProduct(String product) {
        return product != null && product.contains(AXON_SERVER);
    }

    private final LocalDate expiryDate;
    private final Edition edition;
    private final String licenseId;
    private final int contexts;
    private final int clusterNodes;
    private final String licensee;
    private final LocalDate graceDate;
    private final String product;
    private final Set<String> packs;

    LicenseConfiguration(LocalDate expiryDate, Edition edition, String licenseId, int contexts, int clusterNodes,
                         String licensee, String product, String packs, LocalDate graceDate) {
        this.expiryDate = expiryDate;
        this.edition = edition;
        this.licenseId = licenseId;
        this.contexts = contexts;
        this.clusterNodes = clusterNodes;
        this.licensee = licensee;
        this.product = product;
        this.graceDate = graceDate == null ? expiryDate : graceDate;
        this.packs = StringUtils.isEmpty(packs) ? Collections.emptySet() : Arrays.stream(packs.split(",")).collect(
                Collectors.toSet());

    }

    public LocalDate getExpiryDate() {
        return expiryDate;
    }

    public LocalDate getGraceDate() {
        return graceDate;
    }

    public boolean hasPack(String pack) {
        return packs.contains(pack);
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

    public static boolean isEnterprise() {
        return Edition.Enterprise.equals(getInstance().edition);
    }

    private LicenseConfiguration withExpiryDate(LocalDate newExpiryDate) {
        return new LicenseConfiguration(newExpiryDate, edition, licenseId, contexts,
                                        clusterNodes, licensee, product,
                                        String.join(",", packs), graceDate);
    }

    private LicenseConfiguration withGraceDate(LocalDate newGraceDate) {
        return new LicenseConfiguration(expiryDate, edition, licenseId, contexts,
                                        clusterNodes, licensee, product,
                                        String.join( ",", packs), newGraceDate);
    }
}
