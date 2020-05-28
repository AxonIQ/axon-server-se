package io.axoniq.axonserver.licensing;

import java.io.File;
import java.time.LocalDate;

/**
 * Exception to throw when something goes wrong the license file verification, including
 * some useful factory methods.
 *
 * @author Stefan Dragisic
 */
public class LicenseException extends RuntimeException {

    private LicenseException(String message) {
        super(message);
    }

    public static LicenseException expired(LocalDate expiryDate) {
        return new LicenseException("License file expired on " + expiryDate.toString());
    }

    public static LicenseException wrongProduct(String expectedProduct) {
        return new LicenseException("This license does not cover " + expectedProduct + ".");
    }

    public static LicenseException allowedClusterNodesExceeded(int allowedNodes, long currentNodes) {
        return new LicenseException("Cluster limited to " + allowedNodes + " but found " + currentNodes + " nodes. Update number of nodes accordingly and re-upload license.");
    }

    public static LicenseException wrongSignature(String details) {
        return new LicenseException("Could not verify license signature. " + details);
    }

    public static LicenseException noLicenseFile() {
        return new LicenseException("No license found");
    }

    public static LicenseException unableToRead(File file) {
        return new LicenseException(String.format(
                "Unable to read license file. "
                        + "Trying to read from: '%s'", file.getAbsolutePath()));
    }

    public static LicenseException unableToWrite(File file,Throwable e) {
        return new LicenseException(String.format(
                "Unable to write to license file. "
                        + "Trying to write to: '%s' Reason %s", file.getAbsolutePath() , e.getLocalizedMessage()));
    }

}
