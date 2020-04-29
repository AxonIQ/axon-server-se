package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.AxonServerEnterpriseProperties;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.time.LocalDate;
import java.util.*;

import static java.lang.Math.toIntExact;

/**
 * Manages all license operation:
 * <p>
 * Provides access to information from the license key.
 * Provides Read/Write file operations
 * Provides license validation
 * Handles request to update license to newer version
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
@DependsOn({"axonServerEnterpriseProperties"})
public class LicenseManager {

    private static final Logger logger = LoggerFactory.getLogger(LicenseManager.class);
    private static final String PUBKEY =
            "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA0xyNKA5aL42X7eXy1zwe\n" +
                    "9V6do3TYhrH7smWa6RBtCkhQ2holCEalrdiEX3LQoyhPmvV8lqWrDc9JHuYheWQL\n" +
                    "pQXKB84sb9DCCWZWTPV0OZpe8nyotgmwBYohvEzTGLLRrAp+pM/J+/IVSiMyiP5E\n" +
                    "Kf6ODcRWQH/us+4x4IsjTZC+o0HsYjSXG62Bo7pXFXLKjUqA3rpTyT1v3Yafgp4C\n" +
                    "78wHa/fqKCE562B2IEEhxWdsJl//wOsk/I8bYH+YoZtceGpRJlkMjK3t/KOExU61\n" +
                    "ae5NJruyXbqRBWOtrcBb37b2cgykqaZlCwQczsZwl8Pglm1Yl0t8lTdTM+wxLErI\n" +
                    "AbxYE50UtvMLCaIG8lqT9W28UQgOr+RPdkEwUWYNeWWH2R0Kukva9loB+LBDe/Ce\n" +
                    "YhRvh41KpekJhU0NYjymCizNFohQ0rUDtt8p+i/IpIxfWBtgJODOrP2tbr8necX8\n" +
                    "X5oMyN4H/ar6favdWCHXi9FtTrHv1lchisXn3R9/obJptkxyZc8yvWuEBhXBFJ6H\n" +
                    "ydOPNdbiWIH9TptZ2vaQrSFyaPR5yCoG/kyZ6o7TQE8lK6MrULiJNB/6ZKujri5x\n" +
                    "LovNJrtY/w69qVkC/8lIJhwJMSJKySeUYBhOjVN4f7vVEVYncYx8HJU2utQ1j6+e\n" +
                    "9T0pQ8CjhkOpmcTcaaMmU0UCAwEAAQ==";
    private final String LICENSE_FILENAME = "axoniq.license";
    private final String LICENSE_DIRECTORY;
    private static final String AXON_SERVER = "AxonServer";

    public LicenseManager(AxonServerEnterpriseProperties axonServerEnterpriseProperties) {
        LICENSE_DIRECTORY = axonServerEnterpriseProperties.getLicenseDirectory() == null
                ? Paths.get("").toAbsolutePath().toString() : axonServerEnterpriseProperties.getLicenseDirectory();
    }

    @EventListener
    public void on(ClusterEvents.LicenseUpdated licenseUpdated) {
        createOrUpdate(licenseUpdated.getLicense());
    }

    /**
     * Writes license file to the disk or updates overrides current if exists
     *
     * @param license license content
     */
    public void createOrUpdate(byte[] license) {

        logger.info("Validating new license...");

        Properties licenseProperties = loadProperties(license);
        validate(licenseProperties);

        File path = new File(LICENSE_DIRECTORY);
        if (!path.exists() && !path.mkdirs()) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                    "Failed to create directory: " + path.getAbsolutePath());
        }

        try (FileOutputStream out = new FileOutputStream(path + "/" + LICENSE_FILENAME)) {
            out.write(license);
        } catch (IOException e) {
            throw LicenseException.unableToWrite(path, e);
        }

        logger.info("New license saved!");
    }

    /**
     * Validates & reads license file properties
     *
     * @return license properties
     */
    public Properties readLicenseProperties() {
        String licenseFilePath = getLicenseFilePath();
        Properties licenseProperties = loadProperties(licenseFilePath);
        validate(licenseProperties);

        return licenseProperties;
    }

    public byte[] readLicense() {
        logger.info("Loading license bytes...");

        String licenseFilePath = getLicenseFilePath();

        return loadBytes(licenseFilePath);
    }

    @NotNull
    private String getLicenseFilePath() {
        String licenseFilePath = System.getProperty("license", System.getenv("AXONIQ_LICENSE"));

        File defaultLicenseFile = null;

        if(licenseFilePath != null) {
            defaultLicenseFile = new File(licenseFilePath);
        }

        if (defaultLicenseFile == null || !defaultLicenseFile.exists()) {
            licenseFilePath = LICENSE_DIRECTORY + "/" + LICENSE_FILENAME;
            File file = new File(licenseFilePath);
            if (!file.exists()) throw LicenseException.unableToRead(file);
        }
        return licenseFilePath;
    }

    private byte[] loadBytes(String licenseFilePath) {
        logger.info("Loading license...");

        File file = new File(licenseFilePath);

        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            byte[] licenseContent = new byte[toIntExact(file.length())];
            fileInputStream.read(licenseContent);
            return licenseContent;

        } catch (IOException ex) {
            throw LicenseException.unableToRead(file);
        }
    }

    private Properties loadProperties(String licenseFilePath) {
        logger.info("Loading license...");

        File file = new File(licenseFilePath);
        Properties licenseProperties = new Properties();
        try {
            licenseProperties.load(new FileInputStream(file));
        } catch (IOException ex) {
            throw LicenseException.unableToRead(file);
        }
        return licenseProperties;
    }

    private Properties loadProperties(byte[] licenseFileContent) {
        logger.info("Loading license...");

        Properties licenseProperties = new Properties();
        try {
            licenseProperties.load(new ByteArrayInputStream(licenseFileContent));
        } catch (IOException ex) {
            throw LicenseException.noLicenseFile();
        }
        return licenseProperties;
    }

    /**
     * Validates that license content is valid
     *
     * @param licenseContent
     */
    public void validate(byte[] licenseContent) {
        Properties licenseProperties = loadProperties(licenseContent);
        validate(licenseProperties);
    }

    private void validate(Properties licenseProperties) {
        try {
            KeyFactory rsaKeyFactory = KeyFactory.getInstance("RSA");
            byte[] pubKeyBytes = Base64.getMimeDecoder().decode(PUBKEY);
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyBytes);
            PublicKey pubKey = rsaKeyFactory.generatePublic(pubKeySpec);
            Signature rsaVerify = Signature.getInstance("SHA256withRSA");
            rsaVerify.initVerify(pubKey);
            List<String> propnames = new ArrayList<>(licenseProperties.stringPropertyNames());
            Collections.sort(propnames);
            if (!propnames.remove("signature")) throw LicenseException.wrongSignature("signature field missing");
            for (String propname : propnames) {
                rsaVerify.update(propname.getBytes(StandardCharsets.UTF_8));
                rsaVerify.update(licenseProperties.getProperty(propname, "").getBytes(StandardCharsets.UTF_8));
            }
            boolean verifies = rsaVerify.verify(Base64.getDecoder().decode(licenseProperties.getProperty("signature")));
            if (!verifies) {
                throw LicenseException.wrongSignature("signature invalid");
            }

            LocalDate expiryDate = getLocalDate(licenseProperties.getProperty("expiry_date"));
            LocalDate graceDate = getLocalDate(licenseProperties.getProperty("grace_date"));

            if (LocalDate.now().isAfter(Objects.requireNonNull(expiryDate))) {
                if (LocalDate.now().isBefore(Objects.requireNonNull(graceDate))) {
                    logger.warn("License has expired, AxonServer will continue working until {}", graceDate);
                } else {
                    logger.error("AxonServer License has expired");
                    throw LicenseException.expired(expiryDate);
                }
            }
            String product = licenseProperties.getProperty("product");
            if (!validProduct(product)) {
                throw LicenseException.wrongProduct(product);
            }

        } catch (SignatureException ex) {
            throw LicenseException.wrongSignature("SignatureException: " + ex.getMessage());
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException ex) {
            throw new Error("This should never happen", ex);
        }
    }


    public LicenseConfiguration getLicenseConfiguration() {
        Properties properties = readLicenseProperties();
        if (properties == null) {
            throw LicenseException.noLicenseFile();
        } else {
            LicenseConfiguration licenseConfiguration = new LicenseConfiguration(
                    getLocalDate(properties.getProperty("expiry_date")),
                    LicenseConfiguration.Edition.Enterprise,
                    properties.getProperty("license_key_id"),
                    Integer.parseInt(properties.getProperty("contexts", "1")),
                    Integer.parseInt(properties.getProperty("clusterNodes", "3")),
                    properties.getProperty("licensee"),
                    properties.getProperty("product"),
                    properties.getProperty("packs"),
                    getLocalDate(properties.getProperty("grace_date")));

            logger.info("Licensed to: {}", licenseConfiguration.getLicensee());
            logger.info("Running {} mode", licenseConfiguration.getEdition());
            logger.info("License expiry date is {}", licenseConfiguration.getExpiryDate());

            return licenseConfiguration;
        }
    }

    private LocalDate getLocalDate(String dateString) {
        if (StringUtils.isEmpty(dateString)) {
            return null;
        }
        return LocalDate.parse(dateString);
    }

    private  boolean validProduct(String product) {
        return product != null && product.contains(AXON_SERVER);
    }
}
