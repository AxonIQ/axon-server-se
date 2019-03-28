package io.axoniq.axonserver.rest;

import java.time.LocalDate;
import java.util.List;

/**
 * @author Marc Gathier
 */
public class LicenseInfo {
    private LocalDate expiryDate;
    private String edition;
    private String licensee;
    private List<String> featureList;

    public LocalDate getExpiryDate() {
        return expiryDate;
    }

    public void setExpiryDate(LocalDate expiryDate) {
        this.expiryDate = expiryDate;
    }

    public String getEdition() {
        return edition;
    }

    public void setEdition(String edition) {
        this.edition = edition;
    }

    public String getLicensee() {
        return licensee;
    }

    public void setLicensee(String licensee) {
        this.licensee = licensee;
    }

    public List<String> getFeatureList() {
        return featureList;
    }

    public void setFeatureList(List<String> featureList) {
        this.featureList = featureList;
    }
}
