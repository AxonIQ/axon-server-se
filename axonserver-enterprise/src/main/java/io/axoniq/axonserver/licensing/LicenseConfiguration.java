package io.axoniq.axonserver.licensing;

import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides access to information from the license key used.
 * <p>
 * Actual complexity of reading/verifying is delegated to the LicenseManager.
 *
 * @author Stefan Dragisic
 */

public final class LicenseConfiguration {

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

    public boolean isEnterprise() {
        return Edition.Enterprise.equals(edition);
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


    public enum Edition {
        Enterprise("Enterprise edition"), Standard("Standard Edition");

        private final String displayName;

        Edition(String displayName) {
            this.displayName = displayName;
        }

        public String displayName() {
            return displayName;
        }
    }
}
